# MHSDS Currency Models

Models that classify **MHSDS** (Mental Health Services Data Set) activity into the NHSE 2026/27 mental health currencies and attach indicative prices. They answer two questions: *which currency group does each spell or contact belong to*, and *what would that activity cost at national indicative prices*.

Source logic: NHSE "MH Currencies 26-27" grouping SQL (provider version), reimplemented as layered dbt models with the mappings held in seeds. The NHSE code classifies but does not cost; pricing follows the "Basis of Price" column of the NHSE Non-Acute Collection Template price schedule (bed day for inpatient, contact for community/crisis).

## The currency framework in one minute

A currency code has three parts: `MAA98A` = population group (`MAA`) + family (`98`) + setting (`A`).

| Population group | Meaning |
|---|---|
| MAA | Adult – Psychosis and Bipolar Disorders |
| MAB | Adult – Mood and Anxiety Disorders |
| MAE | Adult – Neurocognitive Disorders |
| MAF | Adult – Personality Disorders |
| MAZ | Adult – Cross-cutting crisis |
| MBC | All Age – Eating and Feeding Disorders |
| MBY | All Age – Neurodevelopmental Disorders |
| MCS | CYP – Mental Health Support Teams |
| MCG | CYP – Other |
| MBU | Other / Unclassified |

| Family | Unit | Settings (final letter) |
|---|---|---|
| 98 | Inpatient bed day | A Acute & PICU, B Rehab, C Specialist, D Forensic, Z unknown |
| 96 | Community contact | A Community & Neighbourhood (CMHT), B Specialist, C Forensic, D Day Hospitals & Community Rehab, Z unknown |
| 97 | Crisis contact | A Core Services, B Alternatives, C MH Crisis Assessment Centres, D A&E Linked, Z unknown |
| 99 | Cross-cutting contact | MAZ99A–D by crisis setting, MAZ99Z, MCS99Z |

## How MHSDS data behaves (and why the models look like this)

MHSDS is a **monthly resubmission feed**: providers resubmit every active record each month, and each row belongs to a submission. Analysts should never count raw rows. Every staging model runs `deduplicate_mhsds`, which keeps the latest record per business key **restricted to active submissions** — one clean row per referral, contact, spell, etc.

Two data facts shape the design:

- **Local IDs are not globally unique.** Providers reuse local care-contact IDs across referrals, so the contact grain everywhere is `(uniq_serv_req_id, uniq_care_cont_id)`, never the contact ID alone.
- **Undischarged spells are usually orphans.** Most spells with no discharge date simply stop being submitted (system cutovers, the 2024 BEH/C&I → NLFT merger). `int_mhsds_spell_encounters` classifies each spell's end date as `discharged`, `open` (still being submitted), or `last_submission` (orphaned — closed at its last submission evidence); the currency models reuse that end date rather than re-deriving it.

## Seeds — where the mappings live

All classification lookups are seeds under `seeds/` so analysts can read (and amend) the groupings without touching SQL:

| Seed | Maps |
|---|---|
| `nhse_currency_prices_2627` | Every currency code in the NHSE price schedule → 26/27 indicative price. NULL price = specialised, out of NCC scope. Also carries Talking Therapies, ADHD/ASD and community (CSDS) prices for adjacent work. |
| `nhse_mh_currency_population_groups_2627` | Category letter (A/B/C/E/F/S/Y/Z) → currency group + whether under-18s can take it |
| `nhse_mh_currency_referral_reasons_2627` | `PrimReasonReferralMH` → category |
| `nhse_mh_currency_team_types_2627` | `ServTeamTypeRefToMH` → category, crisis flag, contact setting (96/97 suffix) |
| `nhse_mh_currency_bed_types_2627` | `MHAdmittedPatientClass` (v5 2-digit and v6 3-digit codes) → category + inpatient setting (98 suffix) |
| `nhse_mh_currency_icd10_groups_2627` | 3-character ICD-10 ranges → category |

## Staging — `models/staging/commissioning/mhsds/`

- `stg_mhsds_primdiag` — one row per referral per diagnosis timestamp. Resolves each MHS604 primary diagnosis to a 3-character ICD-10 code: ICD-10-coded rows (99.9%) pass through; SNOMED-coded rows map via the UK SNOMED→ICD-10 complex map refset. Normalisation strips the dot, replaces X with 0, takes 3 characters (e.g. `F32.1` → `F32`).
- `stg_mhsds_servicetype` — one row per referral: the latest service/team type. MHS102 where present, falling back to the v6 `ServTeamType` field on MHS101 (~23% of referrals only have the latter).
- `stg_mhsds_spell`, `stg_mhsds_carecontact`, `stg_mhsds_referral`, `stg_mhsds_mhs502wardstay` — pre-existing deduplicated staging, extended with age and linkage columns.

## Classification — `models/modelling/commissioning/mh_currencies/`

Both models classify with the same **three-tier cascade**. Each tier is consulted only if every earlier tier could not classify:

1. **Diagnosis** — latest primary diagnosis on or before discharge (spells) / the contact date (contacts), categorised by ICD-10 range.
2. **Bed type** (spells) / **team type** (contacts) — from the latest ward stay's admitted patient class, or the referral's service team type.
3. **Referral reason** — `PrimReasonReferralMH`.

**Children (under 18 at admission / at contact)** can only land in the all-age groups (MBC, MBY, plus MCS for MHST contacts). A child whose diagnosis classifies to an adult-only group (e.g. psychosis) does not fall through — they go to `MCG` (CYP – Other), exactly as in the NHSE logic. Unclassifiable adults go to `MBU`.

- `int_mhsds_spell_currency` — one row per hospital spell. Currency code = group + `98` + the setting of the latest ward stay (`Z` if unknown). Carries every input (`diagnosis_category`, `bed_category`, `referral_reason_category`, `winning_tier`) so analysts can see *why* each spell classified as it did.
- `int_mhsds_contact_currency` — one row per (referral, contact), excluding contacts that fall inside an inpatient spell window for the same referral. Family and setting come from the team type: community teams → `96A–D`, crisis teams → `97A–D`, MAZ → `99A–D` by crisis setting, MHSTs → `MCS99Z`; teams with no setting fall to `96Z`/`97Z` by the crisis-referral flag (A18 single points of access count as crisis only for urgent/emergency priority referrals).

## Pricing — `models/reporting/commissioning/mh_currencies/`

Prices are 2026/27 indicative prices (from the 24/25 National Cost Collection). To cost historic activity each price is:

1. **Resolved** through a fallback chain — exact code → the population's `Z` (unknown-setting) price → the MBU (unclassified) price for that setting → MBU `Z`. Needed because specialised settings (e.g. all forensic, all eating-disorder inpatient) are out of NCC scope and unpriced, and some derivable codes (e.g. `MAZ98x`) have no published price. `price_source` records which step supplied the price.
2. **Rebased** to the activity's own fiscal year with the GDP deflator (`uk_cost_indices`), so 2019 activity is costed at 2019-equivalent prices.
3. **Adjusted** by the provider's Market Forces Factor (`nhse_provider_mff_2627`, 1.0 where unknown).

- `fct_mhsds_currency_bed_days` — one row per spell × fiscal year. Nights are attributed to the fiscal year in which they start and sum to the spell length; `proxy_cost` = nights × resolved price × MFF × deflator ratio. Same-day spells count one bed day.
- `fct_mhsds_currency_contacts` — one row per (referral, contact). Attended contacts (status 5, 6, or missing) are costed; DNAs and cancellations are kept with `proxy_cost = 0` so activity counts remain complete.
- `fct_mhsds_current_inpatients` — one row per open spell: who is in a mental health bed now, where, for how long, and under which currency. "Open" means the spell is still being submitted with no discharge date; orphaned undischarged spells are excluded.

On `fct_mhsds_currency_bed_days`, `bed_days_from_date`/`bed_days_to_date` give the exact window each row's bed days cover (to-date exclusive), alongside the whole spell's `spell_start_date`/`spell_end_date` for context — the pairs differ only for spells crossing fiscal years.

## Caveats analysts should know

- **These are proxy costs**, not payments: indicative national prices applied to activity, suitable for comparative and distributional analysis, not for reconciling contracts.
- **~27% of spells and ~25% of contacts are unclassified** (`MBU`) — driven by missing diagnosis/team-type recording, consistent with national MHSDS completeness. They still cost at MBU prices.
- **Provider-submitted currencies can't validate this**: the MHS013 currency model table is empty in our feed, so classification is derived only.
- **Legacy long-stay spells** (admissions back to the 1970s, mostly forensic/LD) accrue decades of bed days and are included; filter on `start_date` or `end_date_source` if they distort a cut.
- The FY2022/23 dip in contact volumes is a **source completeness artefact** (two providers' submissions), not a real activity change.
- `fct_mhsds_currency_bed_days` totals reconcile within ~0.3% of `int_mhsds_spell_encounters` (the earlier MBU-only costing), which remains in place for encounter-level use.
