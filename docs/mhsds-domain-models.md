# MHSDS domain models

These models expose MHSDS records at their submitted business grains. They
support source analysis and provide inputs to cross-system event and clinical
record models.

Definitions follow the current
[MHSDS v6 ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance).
Historical records retain their submitted MHSDS version because fields and
relationships change between versions.

## Submission and time rules

MHSDS is a monthly resubmission feed. Reporting models use active provider
submissions and retain the row from the newest reporting period for each stated
business key. Source receipt time resolves ties within a reporting period.

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

`mhsds_referral_terminology` and `mhsds_care_contact_terminology` supply current
UKHFD descriptions for submitted codes. Their `_history` models retain
definition revisions. Descriptions remain null when a submitted code is absent
from UKHFD; this is most common for historical consultation-medium codes.

## Published interfaces

| Model | Grain | Use |
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

## Nearby source facts

The referral and contact tables are the start of the domain, not a replacement
for their child records. The following source grains should be published as
separate facts or relationships.

| Source | Proposed model | Grain and role |
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
have stable source grains. Their row count is not a reason to fold them into a
parent fact.

These facts need the matching UKHFD code sets before publication. UKHFD holds
the referral reason, RTT status, onward-referral reason, care-contact attendee,
indirect-activity person, consultation medium, cancellation, advocacy and other
MHSDS dictionaries. dbt reference models should expose the current code set and
its history in `REFERENCE.TERMINOLOGY`.

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
