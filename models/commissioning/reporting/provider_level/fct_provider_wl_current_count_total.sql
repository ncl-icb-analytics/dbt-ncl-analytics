{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient, number of waiting lists at unique providers, 
number of waiting lists under unique TFCs, and a flag for whether the patient has an open waiting list for the same TFC under multiple providers.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    wl.provider_code,
    COUNT(*) AS wl_current_total_count,
    IFF(p.provider_code IS NOT NULL, TRUE, FALSE) AS "is_ncl_provider"
FROM {{ ref('int_wl_current') }} wl
LEFT JOIN DEV__MODELLING.LOOKUP_NCL.PROVIDER_SHORTHAND p ON wl.provider_code = p.provider_code
GROUP BY 
ALL