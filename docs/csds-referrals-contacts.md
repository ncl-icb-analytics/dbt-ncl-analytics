# CSDS referrals and contacts

The CSDS facts retain the latest reported referral or referral/contact pair
across all accepted reporting months. They provide source records for the
shared event work in #1006. They do not infer a care journey or apply a programme
population. #854 covers this part of the domain.

## Source authority

The [CSDS tools and guidance](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/community-services-data-set/implementing-the-community-services-data-set-csds-v1.6-tools-and-guidance)
provide the [v1.6.10 ETOS](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
and [user guidance v1.9](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_v1.6_user_guidance_v1.9.pdf).
The ETOS defines provider-qualified identifiers and successor rules. Source
profiles below check those rules against the received data, including versions
1.0 and 1.5. UKHFD supplies national code descriptions and definition history.

## Models and grains

| Model | One row represents |
|---|---|
| `stg_csds_activesubmission` | One accepted submission, enriched with its header and dataset version. |
| `stg_csds_referral_history` | One referral in an accepted submission. |
| `stg_csds_care_contact_history` | One contact in an accepted submission. |
| `stg_csds_cyp101referral` | Latest reported state of a provider-qualified referral identifier. |
| `stg_csds_cyp201carecontact` | Latest reported state of a referral/contact identifier pair. |
| `fct_csds_referral` | The latest referral, with source provenance, its own patient link and national labels. |
| `fct_csds_care_contact` | The latest referral/contact pair, with source provenance, its own patient link and national labels. |
| `csds_referral_code_lookup` | Newest definition of a referral reason, priority or professional-group code, including retired codes. |
| `csds_referral_code_lookup_history` | One UKHFD definition revision for a code. |

The existing patient summaries and currency model have narrower purposes. They
cannot supply complete event records or same-submission child links. The history
and latest models form one staging interface for each source table. Only the
history reads raw rows; it first keeps submissions listed in ActiveSubmission.
The person bridge is an independent identity feed.

Latest selection orders by reporting-period end, file receipt time, submission
identifier and submitted-row identifier, descending with missing dates last.
The last two fields make ties deterministic. `record_number` identifies a
patient within a submission; it is not source row order and is not used to
choose a later correction.

Referral identifiers occasionally move between people in source history. The
latest fact retains the newest reported state, not a complete lifetime history
of each person who previously carried that identifier. Contact identifiers are
also reused across referrals. Contact keys therefore include the referral;
activity links must use the accepted contact history and submission identifier.

## Analyst-facing rules

Dates and times remain those supplied by CSDS. Date-only timestamps use midnight
and carry `date` precision; supplied times carry `timestamp` precision. Receipt
time and reporting months remain separate provenance fields. No arbitrary early
date threshold is imposed. Age is source-derived at the referral/contact date,
including source special values.

Both facts use their own person identifier to obtain the cross-system patient
key. Missing links remain in the population. The contact's recorded referral
key is retained with separate existence and person-agreement flags. A matching
referral key does not establish that both records describe the same person.
Current referral details are not copied onto a conflicting contact.

`attendance_code` uses the original submitted attended-or-did-not-attend field.
The later `attendance_status` field is retained separately as
`source_attendance_status_code`. Where both are populated they agree after
zero-padding is removed. The later field is absent throughout version 1.0 and
mostly absent in 1.5. The new fact retains unknown, cancelled and did-not-attend
records. Existing currency and encounter attendance rules are unchanged.

Submitted consultation mechanism and older consultation medium remain separate.
No cross-version mapping is guessed. National labels use the newest definition,
including retired codes, rather than implying a historically effective label.
Unmatched codes remain visible with a null description. Shared national contact
code definitions reuse the existing MHSDS-named reference interface; they are
not MHSDS-specific clinical rules.

Commissioner fields distinguish submitted commissioner from source-derived ICB,
sub-ICB and derivation reason. The facts apply no current-footprint or GP-practice
fallback. Programme attribution belongs downstream.

## Validation on 8 September 2026

All warehouse outputs used for validation were non-identifying aggregates.
The accepted feed covered October 2017 through July 2026.

| Check | Result |
|---|---:|
| Accepted submissions, headers and provider-months | 15,116 each; no duplicate or missing header period |
| Accepted referral occurrences | 99,880,448 |
| Latest referral identifiers | 15,325,459 |
| Referral identifiers with different people across history | 31,284 |
| Accepted contact occurrences | 61,495,117 |
| Latest referral/contact pairs | 61,490,763 |
| Contact identifiers attached to multiple referrals | 830,644 |
| Referral/contact pairs with different people across history | 5 |
| Duplicate submission/referral keys | 0 |
| Duplicate submission/contact keys | 0 |
| Missing current referral links from contact fact | 0 |
| Contact links with a different current referral person | 48,947 |

The bridge contains 8,304,841 unique non-null person identifiers. New facts
preserve every selected staging row. Field population checks found no wholly
empty columns in the new facts. Older/current provider, referring staff-group
and discharge-letter aliases agreed wherever populated across dataset versions.

Compared with receipt-first selection, period-first selection changes 338,251
referral rows and 21 contact rows. It selects a different discharge date for
37,381 referrals. The source correction reaches existing consumers:

| Production versus rebuilt DEV | Change |
|---|---:|
| Referral summary rows | None: 4,131,614 |
| Referral count | None |
| Open referrals | -36,450 |
| Discharged referrals | +36,450 |
| Currency contact rows | None: 61,490,763 |
| Currency code | 214 contacts |
| Costed attendance eligibility | 5 contacts |
| Proxy cost | 197 contacts; net +£4,711.13 |
| Contact date | 21 contacts |
| Contact person or patient bridge | None |

These are comparisons with the available production tables, not a claim that
all reference inputs were frozen. The newer-period source records explain why
selection can change dates, discharge status, referral reason and currency.
The currency formula and attendance policy have not been edited.

Legacy `REPORTING.MAIN_DATA` facts start on 1 April 2021. The new referral fact
has 9,918,636 records from that date, exactly the legacy referral count, plus
5,406,823 earlier referrals. The contact fact has 39,567,181 records from that
date versus 39,567,175 in the legacy table, plus 21,923,582 earlier contacts.
Contact/person/date reconciliation matches 39,567,160 rows on each side. The
remaining 21 new and 15 legacy rows explain the six-row difference across the
cutoff after latest-period selection. All 9,918,636 legacy referral identifiers
match, with 16 changed person identifiers and 30,196 changed discharge dates;
referral start dates agree. Contact IDs alone are reused, so a join on that
identifier would multiply rows.

Permanent tests cover model grains, latest reporting-period selection, complete
fact populations including unknown attendance, and date/time precision. The
aggregate analyses in `analyses/community/csds_referral_contact_*` retain the
source, selection and output profiling queries for future deliveries.

Targeted DEV builds passed for accepted histories, latest staging, references,
the two facts and existing currency, encounter and patient-summary models.
The downstream build also passed for monthly cost/activity, trajectories,
C-LTCS outputs and its membership snapshot: 8 models, 1 snapshot and 30 tests,
with the existing EPD prescribing-coverage warning. Compile configuration and
expired optional platform-credential warnings did not prevent Snowflake builds.

Builds use the tracked `dev` target and existing shared DEV layers. No task
schema or target override was added; `dbt_project.yml` is unchanged.
