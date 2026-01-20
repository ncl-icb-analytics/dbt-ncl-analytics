{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.SLAM_PODGroup \ndbt: source(''dictionary_dbo'', ''SLAM_PODGroup'') \nColumns:\n  SK_SLAMPODGroupID -> sk_slampod_group_id\n  Trust_Code -> trust_code\n  POD_Code -> pod_code\n  CSU_POD_Code -> csu_pod_code\n  SLAMHRGCode -> slamhrg_code\n  SLAMSpecCode -> slam_spec_code\n  PBR_NPBR -> pbr_npbr\n  SBSCostCentre -> sbs_cost_centre\n  AdHoc_Code -> ad_hoc_code\n  CCG_Code -> ccg_code\n  Site_Code -> site_code\n  Phasing -> phasing\n  DateCreated -> date_created\n  BeginMonth -> begin_month\n  EndMonth -> end_month\n  BeginYear -> begin_year\n  EndYear -> end_year"
    )
}}
select
    "SK_SLAMPODGroupID" as sk_slampod_group_id,
    "Trust_Code" as trust_code,
    "POD_Code" as pod_code,
    "CSU_POD_Code" as csu_pod_code,
    "SLAMHRGCode" as slamhrg_code,
    "SLAMSpecCode" as slam_spec_code,
    "PBR_NPBR" as pbr_npbr,
    "SBSCostCentre" as sbs_cost_centre,
    "AdHoc_Code" as ad_hoc_code,
    "CCG_Code" as ccg_code,
    "Site_Code" as site_code,
    "Phasing" as phasing,
    "DateCreated" as date_created,
    "BeginMonth" as begin_month,
    "EndMonth" as end_month,
    "BeginYear" as begin_year,
    "EndYear" as end_year
from {{ source('dictionary_dbo', 'SLAM_PODGroup') }}
