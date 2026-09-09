# CSDS clinical records

`fct_csds_clinical_record` supplies clinical detail for #1018 and the later
cross-source work in #1006. One row is a retained coded immunisation, an accepted
assessment occurrence or a populated procedure, finding or observation from a
care activity. It is not an encounter or a whole-questionnaire table.

## Source meaning and selection

The implementation uses the
[CSDS ETOS v1.6.10 workbook](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
and [user guidance v1.9](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_v1.6_user_guidance_v1.9.pdf).
Accepted submissions follow `stg_csds_activesubmission`. Staging retains their
source occurrences, including repeated reporting periods.

CYP501 records a coded immunisation on an administration date. It includes
childhood records and adult records when submitted. ETOS C501D37 defines the
successor key using national person, provider, procedure and date. The clinical
model also keeps scheme boundaries, although profiling found no scheme changes
within these keys. It selects the latest accepted reporting evidence and records
the first and last reported periods and the number of represented source rows.
Missing people or incomplete clinical keys retain individual occurrences.

Provider-local identifiers do not define additional immunisations. The source
profile found 581,592 candidate national-person keys with different local
identifiers across reporting periods. The retained row preserves its own local
identifier without treating each changed identifier as a new immunisation.

CYP609 records an assessment completed outside a specific contact. Its submitted
completion date has date precision. Current ETOS C609D37 uses referral, concept
and score to derive source supersession. The historical CSDS_1.0 column in
Complex Derivations used completion date instead of score. Neither source rule
defines the required longitudinal clinical history: 41 current successor keys
contain different completion dates. The clinical model therefore preserves all
accepted assessment occurrences, including equal scores on different dates.

CYP612 links directly to a care activity, despite its "Contact" table name.
It has no submitted assessment date. Its `dmic_observation_date` is a warehouse
addition absent from ETOS. The model keeps that date separately and inherits
contact time through the same accepted activity only when populated person
identifiers agree across assessment, activity and contact. CYP202 components
likewise require a person-consistent contact before inheriting its time.
Missing or conflicting identity does not authorise time inheritance.

Complex Derivations lists a person, provider, concept and score key for CYP612,
but the group has no RecordStartDate or RecordEndDate fields. Profiling found
995 such keys across different activities. The clinical model retains accepted
occurrences rather than collapsing those contacts.

CYP202 clinical components reuse `fct_csds_care_activity` and its same-submission
contact. Observations with values but no code survive. Staff relationships do
not enter the clinical fact. Guidance assigns scored assessments to CYP609 or
CYP612; a recognised assessment code in a CYP202 component receives a flag and
keeps its submitted record type.

## Labels and assessment interpretation

Scheme numbers are field-specific. Procedure 06, finding 04 and observation 03
mean SNOMED CT. Procedure 04/05, finding 02/03 and observation 01/02 mean Read v2
and CTV3. Finding 01 means ICD-10. CSDS still accepts these legacy schemes; the
MHSDS fixed-SNOMED observation rule does not apply.

UKHFD supplies scheme labels. Exact SNOMED and normalised ICD-10 lookups reuse
the shared references. Dictionary.dbo.ReadCodes supplies Read v2 and CTV3 terms
through explicit membership flags. Case and significant dots remain unchanged.
Exact dictionary codes take precedence over source-provided alternative codes;
ambiguous alternatives remain unresolved. Source master and mapped fields were
empty across accepted CYP202 and CYP501 records, so these dictionary mappings
are labelled separately as reference enrichment.

The Read dictionary has 310,652 distinct exact-case codes, including 157,238
Read v2 memberships and 276,841 CTV3 memberships. There are no duplicate code
or alternative-code keys within either system. Of the recorded SNOMED mapping
values, 239,560 are non-positive placeholders. The shared reference exposes
these as absent mappings rather than publishing them as clinical concepts.
Unmatched clinical codes remain visible. Numeric appearance does not establish
SNOMED provenance.

Observation units use `clinical_unit_of_measurement`, with exact-case UCUM
codes, dictionary symbols and recognised aliases. The model does not convert
values or infer a unit from similar typography.

The reproducible CSDS assessment seed has 385 public response or range
definitions for 175 concepts. Only three concepts occur in the existing MHSDS
seed, so the CSDS reference has its own source and published domains. The
generator carries concept and precision through continuation rows. It preserves
the workbook row and collection start date.

ASQ-3 rows represent dimension totals, with a published range of 0–60 and one
decimal place. ASQ:SE publishes integer totals of 0–465. EQ-5D-5L response 9
means "Missing value" for its five dimensions, while Clinical Frailty Scale
response 9 means "Terminally ill". Non-score interpretation belongs to the
concept and response pair. No numeric range is inferred for the EQ-5D index
whose published value says "See comments".

The fact retains submitted text, a technical numeric parse and its parse state.
Score interpretation checks plain decimal syntax and significant fractional
digits before matching numeric responses. Values rounded at scale nine and
unverified numeric formats do not receive interpreted scores. It does not use
floating-point equality to decide whether a response is exactly an integer.
`assessment_score_numeric` includes only enumerated numeric responses or values
within the published range and precision, excluding explicit non-score
responses. An unmatched response remains available as source evidence. Reference
membership and interpreted scores are not retrospective submission validation.

Regenerate the public seed after downloading the named workbook:

```powershell
python scripts/reference/generate_csds_assessment_seed.py --spec logs/csds_etos_v1.6.10.xlsx --output seeds/csds_assessment_scale_definitions.csv
```

The seed contains public specification metadata.

## Identity and validation

The clinical item's `source_record_id` equals `clinical_record_id` and supports
a one-row cross-source drill-down join. `originating_source_record_id` preserves
the immunisation revision or accepted source occurrence. An activity occurrence
can have several clinical components. Assessment occurrence keys change when
a replacement submission replaces the source row; they do not claim a durable
match between indistinguishable assessments.

Initial shared-DEV validation built six models and one seed, with all 11 selected
tests passing. The immunisation model retains 2,460,465 unique clinical items
representing all 16,137,212 accepted source occurrences. All 14 occurrences
without national person identifiers remain separate. Submitted clinical dates
are preserved without importing a date cutoff from MHSDS. Duplicate old and
renamed procedure scheme fields had no differences, so staging exposes the
community-care field once.

Permanent tests cover grain, represented-source reconciliation, clinical
component membership, drill-down identity, date precision, safe parent-time
inheritance, response meanings and published ranges. Singular tests return the
failing model keys for warehouse diagnosis. Agent-visible validation uses only
non-identifying aggregates.
The checked-in `analyses/community/csds_clinical_record_profile.sql` supplies
aggregate coverage, parent, time, response and label checks.


## Initial shared-DEV evidence on 8 September 2026

The final model build passed six models and 16 selected tests. It rebuilt the
clinical fact in 21 seconds. A subsequent run passed all eight singular tests
after adding source-key membership and dictionary preservation checks. Five
synthetic numeric cases also passed, including a value just below nine with
18 fractional digits that must not become Frailty score nine. The reporting
fact has 37,606,281 rows and the same number of distinct clinical IDs.
`dbt ls -s fct_csds_clinical_record+` found no downstream business models.

| Clinical record type | Retained rows | Populated code without a label | Missing clinical time |
|---|---:|---:|---:|
| Immunisation | 2,460,465 | 1,790,007 | 0 |
| Referral assessment | 132,672 | 0 | 0 |
| Activity assessment | 807,956 | 0 | 10 |
| Procedure | 9,373,770 | 411,404 | 172 |
| Finding | 8,923,846 | 988,476 | 140 |
| Observation | 15,907,572 | 3,581,720 | 74 |

All 37,606,281 records have provider labels. Responsible-organisation labels
cover 2,454,889 of 2,457,238 populated responsible-organisation codes. The fact has
30,200,514 clinical descriptions. Dictionary alternatives resolve 3,169,266
Read-coded items while preserving their submitted codes. Separate positive
Read-to-SNOMED mappings cover 3,319,056 items, with 3,280,556 mapped descriptions.
Unmatched codes stay visible; this work does not infer expressions, repair Read
codes or assert that dictionary membership establishes clinical validity.

Every assessment activity parent linked in the same accepted submission. No
populated assessment/activity/contact person identifiers disagreed. The 396
missing inherited times arise from incomplete person identity. CYP612 warehouse
derived dates agreed with the safely inherited contact dates wherever both were
available. There are 10,258 clinical records without a linked patient key.

The assessment references recognise all 940,628 submitted assessment concepts.
They interpret 940,526 scores: 938,773 within published ranges and 1,753
published enumerated responses. Another 102 responses remain unmatched. No
explicit non-score response occurs in the current assessment population; public
reference tests protect the distinction between EQ-5D missing value 9 and
Frailty score 9. The 128,599 recognised assessment concepts submitted as CYP202
components remain procedure, finding or observation records.

There are 634,160 observations with a value but no code. Of 5,157,054 populated
observation units, 3,000,170 receive an exact UCUM, dictionary-symbol or recognised
alias label; 2,156,884 remain unresolved. Values remain in submitted units.
The numeric parser labels 16,196,924 values numeric, 21,408,891 absent and 466
non-numeric or outside the supported numeric representation.

These counts describe the accepted source population at validation time. They
are not fixed data-volume assertions. The legacy contact-activity extract joins
a separate coded-item table and other sources; its row count is not the grain
of either the care-activity fact or this clinical fact.


## Follow-up profiling on 9 September 2026

Profiling covered all 65 original reporting columns. Every column had populated
values and none contained blank strings. The historical date and time checks
found no dates before 1900, future dates, dates after the reporting period or
conflicts between stored time and declared precision. No new date cutoff was
introduced. Code lookups had no duplicate organisation, Read or SNOMED keys.

The analyst interface now uses `submission_id`, `referral_id`, `activity_id` and `contact_id`.
`clinical_code_system` gives a consistent SNOMED CT, Read v2, CTV3 or ICD-10 name.
The field-specific submitted scheme code and authoritative `coding_scheme_name`
remain separate. Clinical time uses the submitted contact relationship even
when a later contact revision changes the current parent.
`is_submitted_contact_person_consistent` names that historical comparison
explicitly. Activity components with a missing person ID return null for person
consistency with their originating activity, rather than asserting agreement.
This corrects 386 prior true flags: 172 procedures, 140 findings and 74 observations.

The derived-date comparison previously returned false when comparison was
unavailable for 36,798,335 records. It now returns null without both dates.
A false result means the two populated dates agree. Organisation names use the
shared latest UKHFD ODS definition, with Dictionary fallback for absent codes.

The largest code gap is historical Read-v2 immunisation delivery. Of 985,817
retained Read-v2 immunisations, 974,621 have unlabelled numeric codes. Those
unlabelled numeric values span 973,198 distinct tokens and occur only in retained
reporting years 2017�2020. The source values do not equal the recorded person,
local patient, source row or record-number identifiers. The raw interface selects
the correctly named immunisation-procedure field. This unusual cardinality needs
delivery-source investigation; a larger terminology dictionary alone does not
explain or repair it. Records and their unmatched status remain intact.

Full UKHFD SNOMED concept and description history adds no matches for the
remaining records submitted as SNOMED CT. Read gaps have no case-only matches,
and source clinical codes contain no outer padding. Some unlabelled CTV3 tokens
occur in UKHFD migration maps, but a mapped target description is not the original
Read term. This change does not relabel a submitted scheme or treat a migration
map as an equivalent source description.

The 2,156,884 unresolved observation-unit records use 200 distinct tokens. Four
numeric tokens account for 1,267,074 records; none matches UKHFD UCUM ConceptID
or the SNOMED concept dictionary. Matching internal dictionary IDs does not
establish source meaning. Another 553,066 records would match after changing
case, but UCUM is case-sensitive. No unresolved token matches a historical UCUM
code or alias omitted by the latest reference. Units remain unchanged until an
authoritative equivalent is available.

The 102 unmatched assessment responses split into 85 outside the published
range, one further response outside the permitted precision, six non-numeric or
outside the numeric representation, and ten unmatched enumerations or values
without a published range. None receives an interpreted assessment score.


The follow-up build passed both changed clinical models and all nine selected
tests in 66 seconds; the clinical fact built in 22 seconds. A fresh profile
confirmed the same 37,606,281 unique records, 30,200,514 clinical descriptions,
940,526 interpreted scores and 3,000,170 unit labels. All 65 reporting columns
have populated values and no blank strings. All provider names now come from
UKHFD ODS API; responsible-organisation coverage remains 2,454,889 records.

There are 36,978,580 named clinical code systems. The remaining 627,701 records
are observations without a submitted scheme. No SNOMED, Read or ICD label is
assigned under a different established system. The date comparison has 807,946
agreements, no disagreements and 36,798,335 unavailable comparisons. No missing
person identifier receives a known activity-consistency result. The 396 missing
clinical times remain absent. Full-stack compilation also passed with 6,888
nodes, including hooks.
