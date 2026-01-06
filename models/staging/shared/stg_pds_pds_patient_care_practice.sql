--CTE with the base staging processing
with base_staging as (
    select
        row_id,
        pseudo_nhs_number as sk_patient_id,
        primary_care_provider as practice_code,
        to_date(primary_care_provider_business_effective_from_date) as event_from_date,
        to_date(primary_care_provider_business_effective_to_date) as event_to_date,
        reason_for_removal,
        der_ccg_of_registration,
        der_current_ccg_of_registration,
        der_icb_of_registration,
        der_current_icb_of_registration
    from {{ ref('raw_pds_pds_patient_care_practice') }}
)

select
    pds.row_id,
    pds.sk_patient_id,
    pds.practice_code,
    pds.event_from_date,
    case 
        when pds.event_to_date is null and rfr.event_to_date is not null then rfr.event_to_date
        else pds.event_to_date
    end as event_to_date,
    --If the reason for removal is NULL, check if it exists in the reason for removal table
    --coalesce(pds.reason_for_removal, rfr.reason_for_removal) as registered_reason_for_removal,
    rfr.reason_for_removal as registered_reason_for_removal,
    der_ccg_of_registration,
    der_current_ccg_of_registration,
    der_icb_of_registration,
    der_current_icb_of_registration

from base_staging as pds

--Join with the reason for removal table
left join {{ ref('stg_pds_pds_reason_for_removal')}} rfr
on rfr.sk_patient_id = pds.sk_patient_id
--If the record of removal occurs during in the duration of another event
and rfr.event_from_date < coalesce(pds.event_to_date, '9999-12-31')
and coalesce(rfr.event_to_date, '9999-12-31') > pds.event_from_date

qualify row_number() over (
    partition by pds.row_id
    order by 
        coalesce(rfr.event_to_date, '9999-12-31') desc,
        rfr.event_from_date desc,
        rfr.row_id desc
) = 1
