-- Staging model for dictionary_dbo.SLAM_PODGroup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
