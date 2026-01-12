{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UCLH_FERTIITY_NCL_2122_2425 \ndbt: source(''reference_analyst_managed'', ''UCLH_FERTIITY_NCL_2122_2425'') \nColumns:\n  FILEID -> fileid\n  DV_FINANCIALYEAR -> dv_financialyear\n  DV_FINANCIALMONTH -> dv_financialmonth\n  DV_PROVIDERCODE -> dv_providercode\n  DV_PROVIDERDESCRIPTION -> dv_providerdescription\n  DV_COMMISSIONERCODE -> dv_commissionercode\n  DV_COMMISSIONERDESCRIPTION -> dv_commissionerdescription\n  DV_ISFREEZE -> dv_isfreeze\n  CONTRACTCODE -> contractcode\n  CONTRACTDESCRIPTION -> contractdescription\n  PODCODE -> podcode\n  PODDESCRIPTION -> poddescription\n  LOCALSPECIALTYCODE -> localspecialtycode\n  LOCALSPECIALTYDESCRIPTION -> localspecialtydescription\n  TREATMENTFUNCTIONCODE -> treatmentfunctioncode\n  DV_TREATMENTFUNCTIONDESCRIPTION -> dv_treatmentfunctiondescription\n  ACTIVITY -> activity\n  COST -> cost\n  BOROUGH -> borough"
    )
}}
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
