# MHSDS domain models

The source facts and relationships keep MHSDS records at their submitted level
of detail. Derived models support analysis and provide inputs to cross-system
event and clinical record models.

Definitions follow the current
[MHSDS v6 ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance).
Historical records retain their submitted MHSDS version because fields and
relationships change between versions.

## How monthly submissions and repeated rows are handled

### 1. Choose the accepted file for each provider-month

Providers can submit more than one file for the same reporting month. Monthly
submission tables are first joined to `stg_mhsds_activesubmission`, which names
the file accepted for each provider and month. This prevents an earlier version
of the whole file being counted alongside its replacement.

This first step does not remove records repeated in later months. The person
bridge is a separate identity-linking feed and does not use the monthly file
rule.

### 2. Decide what a repeat means for that table

What happens next depends on the type of table:

| Type of table | Rows retained by our models | Examples |
|---|---|---|
| Records updated in later submissions | The newest reported version of each record. | Referrals, care contacts, patient indicators, referral-team relationships, legal-status periods, hospital spells and ward stays. |
| Monthly history | Rows from every accepted month. | MHS001 patient history, MHS204 indirect activity and MHS902/MHS903 reference snapshots. |
| Repeated rows needing a table-specific rule | One row using identifiers and ordering defined for that table. | MHS604 uses referral and diagnosis timestamp; MHS903 uses provider, submission and ward code. |

The choice is made from the meaning of the source table. A referral, contact,
legal-status period, spell or ward stay can be reported again with corrected or
newly completed fields. Those rows are versions of the same record. MHS001 and
MHS902 instead describe what was submitted for a month, so their earlier rows
remain useful history.

### 3. Select the newest submitted version where records are updated

Models first identify rows that represent the same record. They then order
those rows by:

1. reporting-period end date, newest first;
2. file receipt timestamp (`effective_from`), newest first;
3. submission identifier, highest first; and
4. source row order or submitted-row identifier where another tie-break is
   needed.

The reporting month and receipt time determine which version is newer. The
remaining fields make the result repeatable when those dates are equal; they do
not imply that one row is clinically more reliable. MHS604 diagnosis also
follows the NHS England grouper order when diagnosis timestamps are equal.

The `select_latest_mhsds_record` macro applies this ordering where tables share
the same pattern. Models with table-specific matching rules apply the same
first three steps directly in their SQL.

`MHSxxxUniqID` fields identify individual submitted rows, so they cannot usually
link versions across months. Referral, contact, spell and ward-stay identifiers
are used where the source provides them.

### 4. Rules applied by the current staging models

| Staging model | Rows treated as the same record | Rows retained |
|---|---|---|
| `stg_mhsds_referral` | Same unique service request | Newest submitted referral |
| `stg_mhsds_carecontact` | Same service request and unique care contact | Newest submitted contact |
| `stg_mhsds_other_service_or_team_type` | Same referral and available service or team identifier | Newest relationship; rows without a service or team identifier are excluded |
| `stg_mhsds_servicetype` | Same referral | Newest referral service information, supplemented from MHS101, MHS102 and MHS902 |
| `stg_mhsds_mhactperiod` | Same unique Mental Health Act episode | Newest submitted legal-status period |
| `stg_mhsds_spell` | Same unique hospital-provider spell number | Newest submitted spell, including a later discharge |
| `stg_mhsds_mhs502wardstay` | Same unique ward-stay identifier | Newest submitted ward stay, including a later end date |
| `stg_mhsds_primdiag` | Same referral and diagnosis timestamp | One primary diagnosis using source and NHS England grouper ordering; rows without a timestamp are excluded |
| `stg_mhsds_mpi_history` | No matching across months | Every MHS001 row from accepted files, identified by its submitted-row id |
| `stg_mhsds_patientindicators` | No matching across months | Every MHS005 row from accepted files because the reporting period is its only time reference |
| `stg_mhsds_indirectactivity` | No matching across months | Every MHS204 row from accepted files because it is activity for that reporting period |
| `stg_mhsds_service_or_team_details` | No matching across months | Every MHS902 team snapshot from accepted files |
| `stg_mhsds_mhs903warddetails` | Same provider, submission and ward code | One ward definition within each accepted file; ward snapshots remain separate across months |

Each model's YAML description states what one row represents, and its tests
check the identifiers expected to be unique after these rules are applied.

### 5. Treat later absence separately from correction

This process gives the newest version seen in an accepted file. MHSDS does not
provide one deletion marker that works across all tables, so a record missing
from a later submission is not automatically treated as deleted. Models for
current occupancy or status also use recorded end dates and recent submission
evidence. An apparently open record is therefore the latest state received, not
proof that the provider still considers it open today.

### MHS001 patient identity needs separate handling

MHS001 is monthly patient history, not a one-row-per-person dimension.
`stg_mhsds_mpi_history` retains every MHS001 row from accepted submissions.
Joining it to activity by `person_id` alone will multiply rows across reporting
periods. Use `stg_mhsds_bridging` for the one-row-per-person link to
`sk_patient_id`, and use `dim_person_mh_profile` for a person-level analytical
summary.

Accepted files from January 2016 to July 2026 contain 16.4 million MHS001
snapshot rows for 1.05 million distinct MHS001 person identifiers. Almost all
of that volume is monthly history: there are 16.39 million distinct
provider-local-patient months. The remaining 9,490 rows repeat that grain and
contain the conflicting identifiers described below. The 2.81 million people
in `stg_mhsds_bridging` are the wider MHSDS-linked population, not an MHS001
patient count.

Some accepted MHS001 files contain more than one row for the same extended
local patient identifier. The rows can disagree on `person_id` and the
pseudonymised NHS number, so source order cannot identify an authoritative row.
The history model retains them and labels the submitted pseudonym separately
from the cross-system patient key supplied by the bridge.

## Date and time precision

Source times arrive attached to the placeholder date `1970-01-01`; staging
keeps only the recorded time. Published `*_at` fields combine the source date
and time. Where no time was supplied, the timestamp uses midnight and the
matching `*_time_precision` field is `date`. A recorded time uses `timestamp`.
Use the precision field to distinguish a date-only record from an event
actually recorded at midnight.

Source dates before 1901 are Excel-epoch missing-value sentinels rather than
plausible clinical dates and are exposed as null.

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

The [MHSDS analytical-domain plan](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1028)
organises the remaining source records around analytical subjects rather than
publishing one model for every numbered source table.

| Source | Analytical model or decision | One row represents and use |
|---|---|---|
| MHS103 Other Reason for Referral | `rel_mhsds_referral_reason` | One referral and additional-reason code. The primary reason remains on the referral fact. |
| MHS104 Referral to Treatment | `fct_mhsds_referral_to_treatment_period` | One logical RTT period using its latest submitted revision. |
| MHS105 Onward Referral | `fct_mhsds_onward_referral` | One dated onward-referral decision and receiving organisation. This is a recorded service-movement signal. |
| MHS106 Discharge Plan Agreement | `fct_mhsds_discharge_plan_agreement` | One dated discharge-plan agreement linked to a referral. |
| MHS202 Care Activity | `fct_mhsds_care_activity` | One care activity linked to its contact and referral. Its populated clinical components can produce separate clinical-record rows. |
| MHS203 Other in Attendance | `rel_mhsds_care_contact_attendee_type` | One contact and attendee-type code. It does not identify another person. |
| MHS204 Indirect Activity | `fct_mhsds_indirect_activity` | One dated activity undertaken for a patient while the patient was not present. It is not a care contact. |
| MHS205 Patient Self-Directed Digital Intervention | Data-quality review in #1032 | A future intervention-period fact if the new v6 records can be linked reliably. |
| MHS206 Staff Activity | `rel_mhsds_care_activity_staff` | One recorded care-activity-to-professional relationship. Missing MHS202 parents remain a visible source-quality state. |
| MHS609 Presenting Complaint | Data-quality review in #1033 | A future clinical-record contribution if its v6 patient and referral links are usable. |
| MHS901 Staff Details | Reporting-period professional context | Staff group, specialty, occupation and job role at the source-defined snapshot grain. |

MHS107 medication prescriptions has no rows in accepted submissions and will
not receive an empty reporting model. It can be added when source data begins
flowing.

Aggregate profiling in August 2026 found about 0.7 million additional referral
reasons, 0.8 million RTT rows, 0.1 million onward referrals, 0.2 million
contact-attendee rows and 1.2 million indirect activities. MHS205 and MHS609
are held behind data-quality reviews because their new v6 records do not
currently link reliably. About 419,000 MHS206 rows refer to an MHS202 care
activity absent from the received data.

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
