-- Staging model for dictionary_dbo.OverseasVisitorStatusClassification
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OverseasVisitorStatusClassificationID" as sk_overseasvisitorstatusclassificationid,
    "BK_OverseasVisitorStatusClassification" as bk_overseasvisitorstatusclassification,
    "OverseasVisitorStatusClassification" as overseasvisitorstatusclassification
from {{ source('dictionary_dbo', 'OverseasVisitorStatusClassification') }}
