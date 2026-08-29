# MHSDS domain models

The source facts and relationships keep MHSDS records at their submitted level
of detail. Derived models support analysis and provide inputs to cross-system
event and clinical record models.

Definitions follow the current
[MHSDS v6 ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance).
Historical records retain their submitted MHSDS version because fields and
relationships change between versions.

## How monthly submissions and repeated rows are handled

MHSDS is a monthly resubmission feed. Selection for monthly submission tables
starts with `stg_mhsds_activesubmission`, which keeps the accepted file for each
provider and reporting period. This removes superseded files for the same
provider-month. It does not remove records repeated in later periods. The person
bridge is a separate identity-linking feed and does not use this rule.

What happens next depends on the type of table:

| Type of table | Rows retained by our models | Examples |
|---|---|---|
| Records updated in later submissions | The newest reported version of each record. | Referrals, care contacts, patient indicators, referral-team relationships, legal-status periods, hospital spells and ward stays. |
| Monthly history | Rows from every accepted month. | MHS001 patient history, MHS204 indirect activity and MHS902/MHS903 reference snapshots. |
| Repeated rows needing a table-specific rule | One row using identifiers and ordering defined for that table. | MHS604 uses referral and diagnosis timestamp; MHS903 uses provider, submission and ward code. |

When a model selects the newest submitted version, our staging rule orders
matching rows by:

1. reporting-period end date, newest first;
2. file receipt timestamp (`effective_from`), newest first;
3. submission identifier.

If this still leaves a tie, the model uses the source row order, the submitted
row identifier or both. MHS604 diagnosis also follows the NHS England grouper
order when diagnosis timestamps are equal.

The `select_latest_mhsds_record` macro applies this ordering where tables share
the same pattern. Models with table-specific matching rules apply the same
first three steps directly in their SQL.

Each model documents and tests the identifiers used to recognise the same
record. `MHSxxxUniqID` fields identify individual submitted rows, so they cannot
usually link versions across months. Referral, contact, spell and ward-stay
identifiers are used where the source provides them.

This process gives the newest version seen in an accepted file. MHSDS does not
provide one deletion marker that works across all tables, so a record missing
from a later submission is not automatically treated as deleted. Models for
current occupancy or status also use recorded end dates and recent submission
evidence.

MHS001 is monthly patient history, not a one-row-per-person dimension.
`stg_mhsds_mpi_history` retains every MHS001 row from accepted submissions.
Joining it to activity by `person_id` alone will multiply rows across reporting
periods. Use `stg_mhsds_bridging` for the one-row-per-person link to
`sk_patient_id`, and use `dim_person_mh_profile` for a person-level analytical
summary.

Some accepted MHS001 files contain more than one row for the same extended
local patient identifier. The rows can disagree on `person_id` and the
pseudonymised NHS number, so source order cannot identify an authoritative row.
The history model retains them and labels the submitted pseudonym separately
from the cross-system patient key supplied by the bridge.

Dates, times and timestamps have separate meanings:

- `*_date` is a Snowflake `DATE`.
- `*_time` is a Snowflake `TIME`; it does not contain the source placeholder
  date `1970-01-01`.
- `*_at` combines the source date and time. A date-only record uses midnight so
  the field remains sortable.
- `*_time_precision` states `date` or `timestamp`. Use it when recorded time
  precision affects an analysis.
- Source dates before 1901 are Excel-epoch missing-value sentinels and are
  exposed as null.

Reference models supply current UKHFD descriptions for submitted codes. Their
`_history` models retain definition revisions. This includes referral, contact,
language, safeguarding and inpatient classification codes. Descriptions remain
null when a submitted code is absent from UKHFD; this is most common for
historical consultation-medium codes. Specialised mental health service
categories remain code-only because their reference list is published outside
UKHFD and is not yet modelled in dbt.

## Published interfaces

| Model | One row represents | Use |
|---|---|---|
| [`fct_mhsds_referral`](../models/reporting/mental_health/fct_mhsds_referral.sql) | One unique service request | Referral receipt, decision, discharge planning, rejection, closure, organisations and primary service context. |
| [`rel_mhsds_referral_service_team`](../models/reporting/mental_health/rel_mhsds_referral_service_team.sql) | One referral, relationship role and service or team | Primary, referred-to and additional teams without multiplying the referral fact. |
| [`fct_mhsds_care_contact`](../models/reporting/mental_health/fct_mhsds_care_contact.sql) | One unique service request and care contact | Patient contact timing, attendance, delivery method, location, booking, accessibility and service context. |
| [`fct_mhsds_referral_episodes`](../models/reporting/mental_health/fct_mhsds_referral_episodes.sql) | One referral | Currency-scoped summary of contacts, indirect activity and inpatient use. Use the source facts when individual records are needed. |
| [`fct_mhsds_currency_contacts`](../models/reporting/mental_health/currencies/fct_mhsds_currency_contacts.sql) | One referral and care contact | Mental health contact currency and proxy cost. |
| [`fct_mhsds_currency_bed_days`](../models/reporting/mental_health/currencies/fct_mhsds_currency_bed_days.sql) | One hospital spell and financial year | Mental health bed-day currency and proxy cost. |
| [`fct_mhsds_current_inpatients`](../models/reporting/mental_health/currencies/fct_mhsds_current_inpatients.sql) | One person with a current spell | Current occupancy using the latest active-submission evidence. |
| [`dim_person_mh_profile`](../models/reporting/mental_health/dim_person_mh_profile.sql) | One MHSDS person | Person-level summary built from the referral, contact and current-inpatient facts, with existing diagnosis, Mental Health Act and safeguarding models. |

Join a contact to its recorded referral using
`fct_mhsds_care_contact.referral_source_record_id =
fct_mhsds_referral.source_record_id`. A valid contact can refer to a service
request absent from the retained referral population, so use a left join.

The referral and contact facts retain the source-derived ICB commissioner and
publish a resolved ICB commissioner. When the source derivation is blank but
the submitted commissioner has the ODS ICB role, the resolved fields use the
submitted commissioner. `icb_commissioner_derivation_method` identifies which
route supplied the value.

`is_wnl_commissioner` provides a present-day footprint filter. It includes the
current WNL ICB, the legacy NCL and NWL ICB and sub-ICB codes, and their 13
predecessor CCGs. It does not classify the London Commissioning Hub as WNL.

## Nearby source facts

The referral and contact tables are the start of the domain, not a replacement
for their child records. The following source records should be published as
separate facts or relationships.

| Source | Proposed model | One row represents and use |
|---|---|---|
| MHS103 Other Reason for Referral | `fct_mhsds_referral_reason` | One additional reason recorded against a referral. Keep primary reason on the referral fact. |
| MHS104 Referral to Treatment | `fct_mhsds_referral_to_treatment_period` | One submitted RTT period or status record linked to a referral. |
| MHS105 Onward Referral | `fct_mhsds_onward_referral` | One onward referral decision, referral date and receiving organisation. This is a recorded service-movement signal. |
| MHS106 Discharge Plan Agreement | `fct_mhsds_discharge_plan_agreement` | One person or group recorded as agreeing discharge-plan content. |
| MHS203 Other in Attendance | `fct_mhsds_care_contact_attendee` | One other attendee recorded against a care contact. |
| MHS204 Indirect Activity | `fct_mhsds_indirect_activity` | One dated activity undertaken for a patient while the patient was not present. It is linked to a referral but is not a patient contact. |
| MHS205 Patient Self-Directed Digital Intervention | `fct_mhsds_self_directed_digital_intervention` | One patient-linked digital intervention period. This source starts in MHSDS v6. |
| MHS206 Staff Activity | `rel_mhsds_care_activity_staff` | One care professional recorded against a care activity. It is clinical-record context, not another contact. |

Aggregate profiling in August 2026 found about 0.7m additional referral
reasons, 0.8m RTT rows, 0.1m onward referrals, 0.2m other-attendee rows and
1.2m indirect activities in active submissions. MHS106 and MHS205 are small but
have clear source identifiers. Their row count is not a reason to fold them
into a parent fact.

These facts need the matching UKHFD code sets before publication. UKHFD holds
the referral reason, RTT status, onward-referral reason, care-contact attendee,
indirect-activity person, consultation medium, cancellation, advocacy and other
MHSDS dictionaries. dbt reference models should expose the current code set and
its history in `REFERENCE.DATA_DICTIONARY`.

## Other MHSDS domains

The following groups need their own model families rather than more columns on
the referral or contact facts:

- MHS002–MHS014: dated GP registration, accommodation, employment, patient
  indicators, care coordination, disability, care plans, social circumstances
  and fit notes. These are person context, not healthcare events.
- MHS401–MHS405: Mental Health Act and community treatment order periods.
- MHS501–MHS518: hospital spells, ward stays, leave, incidents and discharge
  readiness.
- MHS601–MHS609: diagnoses, presenting complaints, assessments and prescribing.
- MHS701–MHS702: Care Programme Approach episodes and reviews.
- MHS901–MHS903: staff, service/team and ward reference entities.

MHS301 group sessions are provider-level activity with no patient-level link.
MHS302 drop-in contacts also sit outside the patient-linked data model in the
MHSDS specification. They can be published for service analysis, but they must
not receive `sk_patient_id` or enter a person event timeline unless a supported
linkage route is established.

## Model boundaries

- Keep source-recorded parent identifiers on child facts.
- Do not turn a one-to-many child table into columns or arrays on its parent.
- Do not treat time proximity as a recorded relationship.
- Keep patient contacts, indirect work and clinical items distinguishable.
- Use current UKHFD descriptions while retaining submitted codes and historical
  validity.
