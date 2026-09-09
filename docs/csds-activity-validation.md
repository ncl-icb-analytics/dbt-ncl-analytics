# CSDS care activities and referral services

`fct_csds_care_activity` preserves one CYP202 activity occurrence per accepted
submission and activity identifier. `fct_csds_referral_service` preserves one
latest reported relationship per referral and nullable local team identifier.
These models deliver #1011 without multiplying the referral or contact facts.

## Source meaning

The [CSDS v1.6 ETOS](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
and [user guidance](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_v1.6_user_guidance_v1.9.pdf)
describe CYP202 as an activity with a lead professional and a contact parent.
They do not provide an independent clinical activity timestamp. The activity
fact therefore exposes the same-submission contact date/time and its precision.
It does not publish delivery metadata as care time or create a separate
healthcare event for each activity.

CYP102 contains service/team relationships under a referral. Its closure and
rejection dates describe that team; they do not close every service under the
referral. The documented key includes a nullable local team identifier. The
latest relationship selects reporting period before file receipt, then uses
submission and source-row identifiers to break ties. All accepted deliveries
remain available in `stg_csds_service_type_history`.

The source evidence differs from the intended global activity-ID uniqueness.
The loaded data reuse activity identifiers across contacts, people and periods.
Submission context therefore forms part of the occurrence key and parent join.
A latest contact or referral may belong to a different source occurrence.
`contact_source_row_id`, `referral_source_row_id` and submission identifiers
retain the precise lineage while the source-record keys support links to the
latest parent facts. Each child retains its own person and patient bridge.

## Aggregate source validation

Profiling on 8 September 2026 used only non-identifying aggregates from active
submissions. Counts describe that source snapshot and will change as data arrive.

| Check | Result |
|---|---:|
| Accepted CYP202 activities | 55,189,449 |
| Duplicate submission/activity keys | 0 |
| Activities with absent or ambiguous same-submission contact | 0 |
| Activities with conflicting populated parent/person identifiers | 0 |
| Activities with unknown parent/person consistency | 396 |
| Accepted CYP102 service/team rows | 97,889,848 |
| Distinct submission/referral/nullable local-team keys | 97,889,843 |
| Duplicated submission/referral/team keys | 5 |
| Duplicated keys with different clinical content | 0 |
| Service rows with absent or ambiguous same-submission referral | 0 |
| Source service rows with conflicting populated parent/person identifiers | 0 |
| Latest relationships with unknown parent/person consistency | 271 |
| Activities with a supplied local professional identifier | 49,411,588 |
| Activities with a source-derived professional identifier | 50,464,170 |
| Activities with observation values | 15,256,762 |
| Activities with UCUM units | 5,161,846 |

The five repeated team keys have identical person, team type, closure/rejection
fields, receipt time and reporting period. History retains their source rows;
the latest relationship keeps one row per logical key. Null team identifiers
remain valid keys and are not discarded.

Procedure, finding and observation scheme aliases with and without the
`_community_care` suffix have zero differences in this snapshot. The two
activity-type aliases also agree everywhere. New interfaces retain one supplied
field for each meaning. All supplied mapped/master clinical codes and preferred
terms are empty in this feed, so these columns are omitted. Original codes,
coding schemes, observation value and unit remain available. The activity fact pairs these codes with scheme-qualified clinical names.
The clinical-record models also provide these items at a long-form clinical grain.

Dictionary joins retain unmatched codes and use the latest available labels.
They do not claim that current labels describe the meaning at every historical
date. Service/team, activity type, closure/rejection reasons and coding schemes
reuse the shared dictionary interfaces.

## Existing consumers

`stg_csds_cyp202careactivity` remains the existing latest contact/activity
projection, with its original file-receipt ordering and output columns. It now
reads the accepted history. `int_csds_activities_summary` and
`int_csds_contacts_activity_summary` continue to consume that projection.
The new occurrence fact preserves more rows deliberately and is not interchangeable
with those latest summaries.

The existing single-team currency selector and its consumers are unchanged.
The relationship fact does not feed a one-to-many join into currency pricing.

## Build and reconciliation evidence

The initial history build passed both source grains. A complete targeted DEV
build then passed five models and 14 tests: the activity and relationship facts,
the existing activity selector and both affected activity summaries. The activity
fact built in 72 seconds and the relationship fact in 64 seconds on
`WH_WNL_ENGINEERING_M`. Final source-count, relationship and history tests passed
all nine selected checks. A final activity build added the supplied same-submission
contact age and passed all six selected tests, including the clinical child checks.
That activity build took 40 seconds. These timings are observations from this build, not
performance guarantees.

The activity fact contains exactly 55,189,449 accepted occurrences. The
relationship fact contains 18,022,117 referral/team keys, including 1,804,685
unspecified teams. There are no missing same-submission parents or conflicting populated person
identifiers in this snapshot. Person consistency is unknown for 396 activities
and 271 latest service relationships, so zero conflicts does not establish
complete patient identification. Permanent linkage tests verify that populated
parent source-row identifiers belong to the same submission and parent key;
unmatched or conflicting source data remain visible rather than being dropped.

An exact symmetric difference over all nine columns of the old raw activity
selection and the rebuilt `stg_csds_cyp202careactivity` returned zero rows.
This checks actual selected values, including tied receipt timestamps, rather
than assuming equivalent window SQL picks identical rows. Both downstream
activity summaries rebuilt and passed their grain tests.

The label checks retained unmatched nonblank codes: 1,412 activity types,
187 finding schemes, 87,244 service/team types, 20 closure reasons and 30,243
rejection reasons. Procedure and observation schemes had no unmatched codes.
These are source coverage findings, not records to discard or relabel by guess.

The reviewed legacy worksheet `1_CSDS_Contact_Activity v1.sql` joins the
CSDS_SIMPLE activity-coding table and emits a code and clinical-item type for
each coding row, with further patient and dictionary joins. Its MAIN_DATA output
therefore has a different grain from the CYP202 activity fact. Matching its
row count would not establish activity completeness; reconciliation of coded
items belongs with the clinical-record models. The new source-occurrence fact
reconciles directly to accepted CYP202 records. Both source-reconciliation tests
compare individual expected and actual logical keys, reporting missing and
unexpected keys even when their row counts balance.

The reusable aggregate-only analysis
[`csds_activity_source_profile.sql`](../analyses/community/csds_activity_source_profile.sql)
rechecks source grains, parent linkage, unknown identities, supplied age and
terminology coverage from the accepted staging and reporting interfaces.
Compile it with the established DEV target; no patient rows are returned.


## Analyst interface review, 9 September 2026

Reporting uses `activity_id`, `contact_id`, `referral_id`, `team_id` and
`submission_id`. Local provider identifiers have a `local_` prefix. Staging
retains source field names. Each fact retains its own `source_record_id` for the
common adapter interface. `referral_id` joins directly to the referral fact's
`source_record_id`; `contact_source_record_id` is the distinct hashed contact key.
Lead professional identifiers are labelled explicitly. CYP901 staffing fields
are not included because their submission-level linkage needs separate source work.

Flags beginning `is_submitted_` describe the source parent from the same
submission. The shorter `is_contact_` and `is_referral_` flags describe the
latest reporting parent. Person agreement stays null when either person is
unknown. Occurrence agreement checks the exact delivered parent row. A false value can
mean a legitimate newer parent revision, not a source error. Dates,
age and patient identity continue to come from the submitted context.

The aggregate review found 1,210 activities whose latest contact is a different
source occurrence with a different date. There were no conflicts between
populated activity and latest-contact person identifiers, and 396 were unknown.
For service relationships, 3,137,089 latest referral occurrences differ,
16,414 latest-parent person identifiers conflict and 275 are unknown. These
rows remain available. Analysts can select the appropriate flags rather than
mistaking historical parent agreement for agreement with the latest fact.
A permanent regression test checks both sets of flags against their named parents.

All provider codes matched the latest UKHFD ODS API names in this snapshot.
The shared organisation reference retains closed and historical organisations
and uses Dictionary names for codes absent from UKHFD. Clinical names use the
supplied scheme to distinguish SNOMED CT, Read v2, CTV3 and ICD-10. Supplied
codes and observation values are preserved when no label exists.

Of 5,161,846 supplied observation units, 3,000,170 matched the existing unit
reference and 2,161,676 did not. Numeric-only source units account for
1,268,303 occurrences. No authoritative meaning was established for these
values, so their labels remain null. The unit name, symbol and definition
source make the available mappings visible.

The review profiled every reporting column for nulls and blanks. No supplied
string fields contained blanks and no output column was wholly empty.
There were 318 activity durations above 1,440 minutes, no negative durations,
and three negative supplied contact ages. No activity contact dates were
outside their reporting periods. Service closure dates exceeded the reporting
period in 33,621 rows and rejection dates in 8,615 rows. These are source
quality findings, not authorised corrections or exclusions.

Trimming, changing case and checking all available historical codebook rows
recovered no missing categorical labels. Padding would recover one service
row, but is not applied as a general coding rule. The remaining unsupported
activity, service, scheme and reason codes retain null names. The reusable
aggregate profile now includes source outliers, clinical and organisation label
coverage, and latest-parent differences.


The post-change DEV build passed all four selected models and 15 tests,
including both individual-key reconciliations and the new submitted/latest
parent flag test. The activity fact built in 68 seconds and the service fact
in 25 seconds. All 85 documented output columns were profiled again: none was
wholly empty and none contained blank strings. Row counts, supplied fields,
source outliers and categorical coverage matched the pre-change baseline.
Both provider-name columns are populated on every row.

| Clinical item | Supplied code | Matched name | Unmatched name |
|---|---:|---:|---:|
| Procedure | 9,373,770 | 8,962,366 | 411,404 |
| Finding | 8,923,846 | 7,935,370 | 988,476 |
| Observation | 15,273,412 | 11,691,692 | 3,581,720 |

The updated committed aggregate profile compiled and executed successfully.
`dbt ls` found no additional reporting consumers on this branch; the stacked
clinical-record PR owns its dependent rebuild. The existing activity selector
and summary models are unaffected by these reporting aliases and label joins.
