-- Raw layer model for reference_analyst_managed.UCLH_FERTIITY_NCL_2122_2425
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "FILEID" as fileid,
    "DV_FINANCIALYEAR" as dv_financialyear,
    "DV_FINANCIALMONTH" as dv_financialmonth,
    "DV_PROVIDERCODE" as dv_providercode,
    "DV_PROVIDERDESCRIPTION" as dv_providerdescription,
    "DV_COMMISSIONERCODE" as dv_commissionercode,
    "DV_COMMISSIONERDESCRIPTION" as dv_commissionerdescription,
    "DV_ISFREEZE" as dv_isfreeze,
    "CONTRACTCODE" as contractcode,
    "CONTRACTDESCRIPTION" as contractdescription,
    "PODCODE" as podcode,
    "PODDESCRIPTION" as poddescription,
    "LOCALSPECIALTYCODE" as localspecialtycode,
    "LOCALSPECIALTYDESCRIPTION" as localspecialtydescription,
    "TREATMENTFUNCTIONCODE" as treatmentfunctioncode,
    "DV_TREATMENTFUNCTIONDESCRIPTION" as dv_treatmentfunctiondescription,
    "ACTIVITY" as activity,
    "COST" as cost,
    "BOROUGH" as borough
from {{ source('reference_analyst_managed', 'UCLH_FERTIITY_NCL_2122_2425') }}
