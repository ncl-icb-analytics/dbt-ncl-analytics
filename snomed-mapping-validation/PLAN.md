# SNOMED CT Concept Map Validation Pipeline

## Problem Statement

The OLIDS concept map (`stg_olids_concept_map`) translates ~1.8M EMIS Web proprietary codes to national SNOMED CT codes. This map was hand-built, with many mappings inherited from the Discovery Data Service (DDS). Some mappings have been shown to be incorrect—wrong target codes, overly broad mappings, or outdated relationships.

**Goal:** Build an automated, multi-pass validation pipeline that:
1. Flags suspect mappings using embedding similarity
2. Validates flagged mappings with LLM reasoning + FHIR terminology context
3. Surfaces results to clinical terminologists for human review

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PREPARATION                             │
│                                                                     │
│  Snowflake ──► Export concept_map (source_display, target_display)  │
│  FHIR/TRUD ──► Export full UK SNOMED CT + Drug Extension descs     │
│                                                                     │
│  Output: parquet files with description_id, concept_id, term        │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     PASS 1: EMBEDDING GENERATION                    │
│                          (Local GPU + API)                           │
│                                                                     │
│  Models:                                                            │
│    ├─ BioLORD-2023 (local, RTX 4090)                               │
│    ├─ SapBERT (local, RTX 4090)                                    │
│    ├─ ClinicalBERT (local, RTX 4090)                               │
│    └─ OpenAI text-embedding-3-large (via OpenRouter API)           │
│                                                                     │
│  Steps:                                                             │
│    1. Embed all UK SNOMED + Drug Extension descriptions             │
│    2. Embed all source descriptions from concept map                │
│    3. Embed all target descriptions from concept map                │
│    4. Build vector index over full SNOMED description space         │
│                                                                     │
│  Output per model:                                                  │
│    ├─ vectors/*.parquet  (description_id → vector)                  │
│    └─ index/ (FAISS or similar ANN index)                           │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PASS 1b: SIMILARITY SCORING                        │
│                                                                     │
│  For each concept map row:                                          │
│    1. Compute cosine similarity between source and target vectors   │
│    2. Search SNOMED index for top-K nearest neighbours to source    │
│    3. Score: does the current target appear in top-K?               │
│    4. Flag if:                                                      │
│       - Source↔Target similarity < threshold                        │
│       - A closer match exists in SNOMED namespace                   │
│       - Multiple models agree the mapping is poor                   │
│                                                                     │
│  Output:                                                            │
│    ├─ scores.parquet (per-model similarity + rank)                  │
│    ├─ flagged_mappings.parquet (union of flagged rows)              │
│    └─ suggested_alternatives.parquet (top-K candidates per flag)    │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│              PASS 2: LLM VALIDATION (Durable Workflow)              │
│                                                                     │
│  Runtime: Cloudflare Workers + Durable Objects                      │
│                                                                     │
│  For each flagged mapping:                                          │
│    1. Retrieve FHIR context for target code:                        │
│       - $lookup on target concept (fully specified name, synonyms)  │
│       - Parents (IS-A relationships)                                │
│       - Children                                                    │
│       - Other relationships (finding site, laterality, etc.)        │
│       - Reference set membership                                    │
│    2. Retrieve same context for top-K suggested alternatives        │
│    3. Prompt LLM with:                                              │
│       - Source description (EMIS)                                   │
│       - Current target + hierarchy context                          │
│       - Alternative candidates + their hierarchy context            │
│       - Tool access: FHIR $translate, $lookup, ECL search           │
│    4. LLM returns structured verdict:                               │
│       - mapping_status: correct | incorrect | ambiguous | review    │
│       - confidence: 0.0–1.0                                        │
│       - suggested_target: (if different from current)               │
│       - reasoning: free text explanation                            │
│       - evidence: list of FHIR queries used                        │
│                                                                     │
│  Durable Object design:                                             │
│    - MappingValidationBatch DO: manages chunks of ~100 mappings     │
│    - State: progress, retries, rate limits, results                 │
│    - Automatic retry with backoff on LLM/FHIR failures             │
│    - Checkpointing: resume from last completed mapping              │
│    - Alarm-based scheduling to stay within rate limits              │
│                                                                     │
│  Output:                                                            │
│    └─ llm_verdicts.parquet / stored in D1 or R2                     │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PASS 3: HUMAN REVIEW WEB APP                       │
│                                                                     │
│  Stack: Next.js or SvelteKit + Cloudflare Pages/Workers             │
│  Auth: NHS Identity / Azure AD / simple auth                        │
│                                                                     │
│  Features:                                                          │
│    ├─ Dashboard                                                     │
│    │   - Summary stats: total flagged, reviewed, accepted, rejected │
│    │   - Filter by: confidence, model agreement, category, status   │
│    │   - Priority queue (lowest confidence first)                   │
│    │                                                                │
│    ├─ Review Interface                                              │
│    │   - Source description + code                                  │
│    │   - Current target description + code + FHIR hierarchy view   │
│    │   - LLM verdict + reasoning                                   │
│    │   - Suggested alternatives with similarity scores              │
│    │   - FHIR terminology browser (inline search)                  │
│    │   - Actions: Accept current | Accept suggestion | Manual map  │
│    │   - Notes field for terminologist commentary                   │
│    │                                                                │
│    ├─ Audit Trail                                                   │
│    │   - Who reviewed, when, what decision, previous state          │
│    │   - Export reviewed mappings for import back to concept map    │
│    │                                                                │
│    └─ Batch Operations                                              │
│        - Bulk accept LLM suggestions above confidence threshold    │
│        - Export corrected mappings as FHIR ConceptMap resource      │
│        - Generate change report for governance review               │
│                                                                     │
│  Data store: Cloudflare D1 (SQLite) or Turso                       │
│  File storage: R2 for parquet/vector files                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Component Design

### 1. Data Preparation

#### 1a. Concept Map Export

Source: `stg_olids_concept_map` joined with `stg_olids_concept`

```
concept_map_row:
  source_code_id        -- FK to concept.id
  source_code           -- EMIS code value
  source_display        -- EMIS description text (what we embed)
  source_system         -- e.g., http://emishealth.com/...
  target_code_id        -- FK to concept.id
  target_code           -- SNOMED CT concept ID
  target_display        -- SNOMED preferred term (what we embed)
  target_system         -- http://snomed.info/sct
  equivalence           -- equivalent, wider, narrower, etc.
  is_primary            -- boolean
  is_active             -- boolean
```

Export as parquet for local processing. ~1.8M rows.

#### 1b. Full SNOMED CT Description Export

Source: TRUD download or FHIR terminology server bulk export.

We need every description (not just concepts) from:
- **UK Clinical Edition** (core International + UK Extension)
- **UK Drug Extension** (dm+d SNOMED concepts)

This gives us the full search space for finding better mappings.

```
snomed_description:
  description_id        -- SNOMED description ID (unique)
  concept_id            -- SNOMED concept ID (many descriptions per concept)
  term                  -- The actual text to embed
  type_id               -- FSN (900000000000003001) or Synonym (900000000000013009)
  language_code         -- en
  active                -- boolean
  module_id             -- identifies UK vs International vs Drug extension
```

Approximate scale:
- ~350K active concepts in UK edition
- ~800K+ active descriptions (FSNs + synonyms)
- ~60K+ dm+d concepts with their descriptions

#### 1c. Data Pipeline Script

```python
# src/data_prep.py
# - Connect to Snowflake, export concept_map to parquet
# - Download/parse SNOMED RF2 files (or FHIR bulk export)
# - Deduplicate, filter active descriptions
# - Output: data/concept_map.parquet, data/snomed_descriptions.parquet
```

### 2. Embedding Models

#### 2a. Local Models (RTX 4090, 24GB VRAM)

| Model | Dimensions | Strengths | Batch Strategy |
|-------|-----------|-----------|----------------|
| **BioLORD-2023** (`FremyCompany/BioLORD-2023`) | 768 | Trained on biomedical ontology descriptions; best for SNOMED-style terms | Batch 512, fp16 |
| **SapBERT** (`cambridgeltl/SapBERT-from-PubMedBERT-fulltext`) | 768 | Biomedical entity linking; strong on synonym matching | Batch 512, fp16 |
| **Clinical-Longformer** or **MedCPT** | 768 | Clinical note embeddings; may catch contextual nuance | Batch 256, fp16 |

All fit comfortably on a 4090. Processing ~1M descriptions at batch 512 takes ~20-40 minutes per model.

#### 2b. API Model

| Model | Dimensions | Access | Rate Limits |
|-------|-----------|--------|-------------|
| **text-embedding-3-large** | 3072 (or 256/1024 with dimension reduction) | OpenRouter API | ~3000 RPM |

At 3000 RPM with batch endpoint, ~1M descriptions would take ~6 hours. Consider:
- Using OpenRouter batch API if available
- Running only on the ~1.8M concept map descriptions (not full SNOMED) for this model
- Or reducing dimensions to 1024 for faster indexing while retaining quality

#### 2c. Embedding Pipeline

```python
# src/embeddings/embed.py
# - Load model onto GPU
# - Stream parquet in chunks
# - Batch encode with progress bar
# - Write vectors to parquet (description_id, vector as list[float])
# - Build FAISS index per model

# src/embeddings/models.py
# - ModelConfig dataclass
# - HuggingFaceEmbedder (local, sentence-transformers)
# - OpenRouterEmbedder (API, httpx async with rate limiting)
```

### 3. Similarity Scoring

```python
# src/embeddings/score.py

For each concept_map row:
    source_vec = lookup(source_description_id, model)
    target_vec = lookup(target_description_id, model)

    # Direct similarity
    pair_similarity = cosine_sim(source_vec, target_vec)

    # Search for better matches
    top_k = faiss_index.search(source_vec, k=20)

    # Where does current target rank?
    target_rank = find_rank(target_description_id, top_k)

    # Best alternative that isn't the current target
    best_alt = top_k[0] if top_k[0] != target else top_k[1]

Output columns:
    - source_code_id, target_code_id
    - model_name
    - pair_cosine_similarity
    - target_rank_in_top_k
    - best_alternative_description_id
    - best_alternative_concept_id
    - best_alternative_similarity
    - best_alternative_term
```

**Flagging criteria** (configurable):
- `pair_cosine_similarity < 0.70` (model-specific thresholds)
- `target_rank_in_top_k > 5` (current mapping not in top 5 nearest)
- Agreement across 2+ models that mapping is suspect
- `equivalence = 'inexact'` or `'unmatched'` in concept map (already known to be weak)

### 4. Durable Workflow (Pass 2)

#### 4a. Why Cloudflare Durable Objects?

- **Checkpointing**: Each mapping validation can be resumed if the worker dies
- **Rate limiting**: Alarm-based scheduling respects LLM and FHIR API rate limits
- **Cost**: Pay-per-request, no idle server costs for a batch job that may run over days
- **State**: Each batch DO holds its progress in durable storage
- **Alternatives considered**: Temporal.io (more complex), simple queue + PostgreSQL (less resilient), AWS Step Functions (vendor lock-in with AWS)

#### 4b. Workflow Design

```
CloudflareWorker (HTTP trigger or cron)
  │
  ├── POST /api/start-validation
  │     Creates ValidationOrchestrator DO
  │     Splits flagged_mappings into batches of 100
  │     Creates MappingBatch DO per batch
  │
  ├── MappingBatch DO
  │     State: { batch_id, mappings[], current_index, results[], status }
  │
  │     alarm() → process next mapping:
  │       1. Fetch FHIR context (parallel requests)
  │       2. Call LLM with structured prompt
  │       3. Parse structured response
  │       4. Store result
  │       5. Schedule next alarm (respecting rate limits)
  │
  │     On completion → notify orchestrator
  │
  ├── ValidationOrchestrator DO
  │     Tracks overall progress across all batches
  │     Aggregates results
  │     Triggers export when all batches complete
  │
  └── GET /api/status
        Returns progress: { total, completed, failed, in_progress }
```

#### 4c. FHIR Terminology Server Integration

The FHIR terminology server provides rich context about each SNOMED concept:

```
GET /CodeSystem/$lookup?system=http://snomed.info/sct&code={sctid}
  → Returns: display, designation (all descriptions), properties

GET /CodeSystem/$lookup?system=http://snomed.info/sct&code={sctid}&property=parent&property=child
  → Returns: hierarchical context

GET /ValueSet/$expand?filter={search_term}&count=20
  → Allows LLM to search for alternative codes by term

GET /ConceptMap/$translate?system=http://snomed.info/sct&code={sctid}
  → Check existing official maps
```

#### 4d. LLM Prompt Design

```
You are a clinical terminology specialist validating SNOMED CT concept mappings.

## Current Mapping
Source (EMIS): {source_display} [{source_code}]
Target (SNOMED): {target_display} [{target_code}]
Stated equivalence: {equivalence}

## Target Concept Context (from FHIR)
Fully Specified Name: {fsn}
Synonyms: {synonyms}
Parents: {parents}
Children: {children}
Relationships: {relationships}

## Alternative Candidates (from embedding similarity)
1. {alt_1_display} [{alt_1_code}] - similarity: {score}
   Parents: {alt_1_parents}
2. {alt_2_display} [{alt_2_code}] - similarity: {score}
   ...

## Tools Available
- search_snomed(term): Search for SNOMED concepts by term
- lookup_concept(sctid): Get full details of a SNOMED concept
- get_hierarchy(sctid): Get parent/child tree for a concept
- check_refsets(sctid): Check reference set membership

## Task
Evaluate whether the current mapping is correct. Consider:
1. Do the source and target represent the same clinical concept?
2. Is the stated equivalence level appropriate?
3. Would any alternative be a better mapping?
4. Are there semantic subtleties (e.g., "history of X" vs "X")?

Return your verdict as JSON:
{
  "mapping_status": "correct|incorrect|ambiguous|needs_review",
  "confidence": 0.0-1.0,
  "suggested_target_code": "sctid or null",
  "suggested_target_display": "term or null",
  "suggested_equivalence": "equivalent|wider|narrower|inexact",
  "reasoning": "explanation",
  "tools_used": ["list of tool calls made"],
  "clinical_notes": "any additional context for human reviewer"
}
```

### 5. Human Review Web App (Pass 3)

#### 5a. Tech Stack

- **Framework**: SvelteKit (lightweight, fast, good for data-heavy UIs)
  - Alternative: Next.js if team prefers React ecosystem
- **Hosting**: Cloudflare Pages (static) + Workers (API)
- **Database**: Cloudflare D1 (SQLite-compatible, serverless)
- **File Storage**: Cloudflare R2 (S3-compatible, for parquet/exports)
- **Auth**: Simple token-based or Azure AD via Cloudflare Access

#### 5b. Data Model (D1)

```sql
-- Core tables
CREATE TABLE mapping_reviews (
    id TEXT PRIMARY KEY,
    source_code_id TEXT NOT NULL,
    source_code TEXT NOT NULL,
    source_display TEXT NOT NULL,
    source_system TEXT NOT NULL,
    target_code_id TEXT NOT NULL,
    target_code TEXT NOT NULL,
    target_display TEXT NOT NULL,
    target_system TEXT NOT NULL,
    original_equivalence TEXT,

    -- Embedding scores (per model)
    biolord_similarity REAL,
    sapbert_similarity REAL,
    openai_similarity REAL,
    avg_similarity REAL,

    -- LLM verdict
    llm_status TEXT, -- correct|incorrect|ambiguous|needs_review
    llm_confidence REAL,
    llm_suggested_code TEXT,
    llm_suggested_display TEXT,
    llm_reasoning TEXT,

    -- Human review
    review_status TEXT DEFAULT 'pending', -- pending|accepted|rejected|remapped
    reviewer TEXT,
    reviewed_at TIMESTAMP,
    review_decision TEXT, -- keep_current|accept_llm|accept_alternative|manual
    final_target_code TEXT,
    final_target_display TEXT,
    reviewer_notes TEXT,

    -- Metadata
    priority_score REAL, -- computed from confidence + model agreement
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE review_audit_log (
    id TEXT PRIMARY KEY,
    mapping_review_id TEXT REFERENCES mapping_reviews(id),
    action TEXT NOT NULL,
    previous_state TEXT, -- JSON snapshot
    new_state TEXT,      -- JSON snapshot
    reviewer TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE suggested_alternatives (
    id TEXT PRIMARY KEY,
    mapping_review_id TEXT REFERENCES mapping_reviews(id),
    rank INTEGER,
    concept_id TEXT,
    description_id TEXT,
    term TEXT,
    similarity_score REAL,
    model_name TEXT,
    fhir_context TEXT -- JSON with parents/children/relationships
);
```

#### 5c. Key Views

**Dashboard:**
```
┌─────────────────────────────────────────────────────────┐
│  SNOMED Mapping Validation                    [Export ▼] │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Total Flagged: 45,230    Reviewed: 12,100 (26.7%)     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │ Accepted │ │ Remapped │ │ Rejected │ │ Pending  │  │
│  │  8,200   │ │  2,450   │ │  1,450   │ │ 33,130   │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘  │
│                                                         │
│  Filter: [Confidence ▼] [Category ▼] [Status ▼] [🔍]  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Source                │ Target        │ Sim │ LLM│   │
│  ├───────────────────────┼───────────────┼─────┼────┤   │
│  │ Hba1c level (EMIS)   │ Hba1c - DCCT  │ .62 │ ⚠️ │   │
│  │ BP systolic (EMIS)   │ Systolic art..│ .84 │ ✅ │   │
│  │ Diabetes type II...  │ Non-insulin ..│ .71 │ ❌ │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Review Interface:**
```
┌────────────────────────────────────────────────────────────────┐
│ Review #12345                              [← Prev] [Next →]  │
├────────────────────────┬───────────────────────────────────────┤
│ SOURCE (EMIS)          │ CURRENT TARGET (SNOMED)              │
│                        │                                       │
│ Code: EMISNQDI123      │ Code: 44054006                       │
│ Term: "Type 2 diabetes │ FSN: "Diabetes mellitus type 2       │
│  mellitus"             │  (disorder)"                         │
│ System: emishealth.com │ Synonyms:                            │
│                        │  - Type II diabetes mellitus          │
│                        │  - Non-insulin dependent DM           │
│                        │ Parents:                              │
│                        │  └─ Diabetes mellitus (disorder)      │
│                        │ Children:                             │
│                        │  ├─ Type 2 DM with complications      │
│                        │  └─ Type 2 DM without complications   │
├────────────────────────┴───────────────────────────────────────┤
│ EMBEDDING SIMILARITY                                           │
│  BioLORD: 0.82  │  SapBERT: 0.79  │  OpenAI: 0.85           │
├────────────────────────────────────────────────────────────────┤
│ LLM VERDICT: ⚠️ NEEDS REVIEW  (confidence: 0.65)              │
│ Reasoning: "The mapping is broadly correct but the target     │
│ concept is the general 'Type 2 DM' code. The source term      │
│ may be better mapped to a more specific concept if the         │
│ clinical context implies active management..."                 │
├────────────────────────────────────────────────────────────────┤
│ SUGGESTED ALTERNATIVES                                         │
│  1. 44054006 - Diabetes mellitus type 2 (current) — 0.85     │
│  2. 313436004 - Type 2 DM — 0.83                              │
│  3. 609568004 - Type 2 DM without complication — 0.78         │
│  [Search SNOMED: _______________] [🔍]                        │
├────────────────────────────────────────────────────────────────┤
│ DECISION                                                       │
│  (●) Keep current mapping                                      │
│  ( ) Accept LLM suggestion                                     │
│  ( ) Select alternative: [dropdown]                            │
│  ( ) Manual: enter SNOMED code [________]                      │
│                                                                │
│  Notes: [                                                  ]   │
│         [                                                  ]   │
│                                                                │
│  [Submit Review]                                               │
└────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
snomed-mapping-validation/
├── PLAN.md                          # This document
├── pyproject.toml                   # Python project config (embeddings pipeline)
├── config/
│   ├── models.yaml                  # Embedding model configurations
│   ├── thresholds.yaml              # Flagging thresholds per model
│   └── fhir.yaml                    # FHIR server connection details
├── src/
│   ├── data_prep.py                 # Export concept map + SNOMED descriptions
│   ├── embeddings/
│   │   ├── __init__.py
│   │   ├── models.py                # Model loading + encoding abstractions
│   │   ├── embed.py                 # Batch embedding pipeline
│   │   ├── index.py                 # FAISS index build/query
│   │   └── score.py                 # Similarity scoring + flagging
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── snowflake.py             # Snowflake connection helpers
│   │   └── fhir.py                  # FHIR terminology server client
│   └── export.py                    # Export results to parquet/D1
├── workflow/                        # Cloudflare Workers project
│   ├── wrangler.toml
│   ├── package.json
│   ├── src/
│   │   ├── index.ts                 # Worker entry point + routes
│   │   ├── orchestrator.ts          # ValidationOrchestrator DO
│   │   ├── batch.ts                 # MappingBatch DO
│   │   ├── fhir-client.ts           # FHIR terminology server client
│   │   ├── llm-client.ts            # LLM API client (OpenRouter/Anthropic)
│   │   └── types.ts                 # Shared TypeScript types
│   └── migrations/
│       └── 0001_init.sql            # D1 schema
├── review-app/                      # SvelteKit web app
│   ├── package.json
│   ├── svelte.config.js
│   ├── src/
│   │   ├── routes/
│   │   │   ├── +page.svelte         # Dashboard
│   │   │   ├── review/
│   │   │   │   └── [id]/+page.svelte # Review interface
│   │   │   └── api/                 # API routes
│   │   ├── lib/
│   │   │   ├── components/          # UI components
│   │   │   ├── stores/              # Svelte stores
│   │   │   └── fhir.ts              # FHIR browser component logic
│   │   └── app.html
│   └── wrangler.toml                # CF Pages config
├── data/                            # Local data (gitignored)
│   ├── concept_map.parquet
│   ├── snomed_descriptions.parquet
│   └── vectors/                     # Per-model vector outputs
└── .cache/                          # Model cache (gitignored)
```

---

## Implementation Phases

### Phase 1: Data Preparation + Local Embeddings
- Export concept map from Snowflake
- Obtain UK SNOMED CT + Drug Extension RF2 files (via TRUD or FHIR bulk)
- Parse and normalize all descriptions into parquet
- Run BioLORD-2023, SapBERT, and one other local model on RTX 4090
- Run OpenAI text-embedding-3-large via OpenRouter
- Build FAISS indices
- Compute similarity scores and generate flagged mappings

### Phase 2: Durable Workflow (LLM Validation)
- Set up Cloudflare Workers project with Durable Objects
- Implement FHIR terminology server client
- Implement LLM prompt with tool-calling capability
- Build orchestrator + batch processing DOs
- Implement D1 database schema
- Run validation over flagged mappings
- Store results

### Phase 3: Human Review Web App
- Set up SvelteKit project on Cloudflare Pages
- Build dashboard view with filtering and stats
- Build review interface with FHIR hierarchy browser
- Implement audit trail and decision capture
- Add batch operations and export functionality
- Deploy and hand off to clinical terminologists

### Phase 4: Feedback Loop
- Export reviewed mappings back to concept map format
- Feed confirmed correct/incorrect mappings back to improve thresholds
- Retrain or fine-tune embeddings on validated pairs (optional)
- Integrate with dbt pipeline for ongoing monitoring

---

## Key Decisions to Make

1. **FHIR server**: Which terminology server? Ontoserver, Snowstorm, HAPI FHIR?
   - Need one that supports UK SNOMED CT edition + Drug Extension

2. **LLM for Pass 2**: Claude (Anthropic) vs GPT-4o vs open-source medical LLM?
   - Claude with tool use is strong for structured reasoning
   - Cost: ~$0.01-0.03 per mapping at current API prices × 45K flagged ≈ $450-1350

3. **Threshold tuning**: Initial thresholds are guesses—need a calibration set of known-good and known-bad mappings to tune

4. **Scope of full SNOMED embedding**: Embed all ~800K descriptions or just FSNs (~350K)?
   - Synonyms increase recall but also increase noise and storage
   - Recommend: embed all active descriptions, index at concept level (take max similarity across descriptions for each concept)

5. **Review app auth**: NHS Identity, Azure AD, or simpler token-based for internal use?

6. **Hosting the review app**: Cloudflare Pages, or internal infrastructure?

---

## Estimated Scale

| Item | Count | Notes |
|------|-------|-------|
| Concept map rows | ~1.8M | Source→Target mappings |
| Unique source descriptions | ~500K-1M | EMIS codes (estimated after dedup) |
| Unique target descriptions | ~200K-400K | National SNOMED subset |
| Full UK SNOMED descriptions | ~800K | All active FSNs + synonyms |
| Drug Extension descriptions | ~60K | dm+d SNOMED content |
| Vectors per model (full set) | ~1-2M | 768d or 3072d per vector |
| Estimated flagged mappings | ~30K-80K | 2-5% of total (TBD after calibration) |
| LLM validations needed | ~30K-80K | One per flagged mapping |
| Human reviews needed | ~5K-20K | Subset where LLM is uncertain |
