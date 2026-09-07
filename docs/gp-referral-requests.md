# GP referral requests

`fct_gp_referral_request` contains one row per retained OLIDS referral request.
`source_record_id` is the original `referral_request.id`, used for direct joins.
Deleted records and records without `person_id` are excluded by staging. The
fact adds no current-registration, referral-direction or date restriction.

The fact preserves original referral, priority, specialty, type and date-precision
codes and displays. Source-supplied mapped referral code, display and coding
system remain separate. Missing terminology is not replaced with a current label.

Requester, recipient and publisher identify different organisation roles. Their
names and codes come from the current retained OLIDS organisation lookup. They
are not historical organisation attributes. The publisher code recorded on the
referral remains separate so discrepancies can be investigated.

`sk_patient_id` comes from the established person pseudonym dimension. Referrals
without a matching key remain in the fact. Encounter IDs and booking references
are recorded links. Neither proves that a consultation occurred or caused later
activity in another system. Clinical date precision remains as supplied; this
model does not infer timestamp precision or repair implausible dates.

## Coverage checked on 8 September 2026

The DEV build retained all 23,590,546 staged referrals, with no missing or extra
keys and no differences in the source fields published by the fact. The staging
extension left its original column values and row count unchanged.

- 23,588,144 referrals have a cross-system patient key; 2,402 remain unlinked.
- 3,259,744 have a non-blank booking reference. No cross-source match is inferred.
- No referral currently supplies an encounter ID or specialty concept ID.
- 23,587,061 have a clinical effective date.
- All referrals have source referral and date-precision displays. All 5,386,757
  recorded priority and type concepts have source displays.
- All referrals with recorded referring or publishing organisation IDs have
  names. Of 5,386,757 referrals with a receiving organisation ID, 512 have no
  retained organisation name.

These are source and lookup coverage limits. They do not justify dropping records
or inventing links. Counts change as the underlying sources refresh.

The reproducible aggregate check is
[`gp_referral_request_validation.sql`](../analyses/olids/gp_referral_request_validation.sql).
It returns counts only, including source reconciliation, recorded encounter
coverage, encounter person-key disagreements, booking references and labels.
Run it through `dbt show --target dev -s gp_referral_request_validation --output json`.
The fact's permanent tests check a non-null unique referral key and equal row count
with its staging input.
