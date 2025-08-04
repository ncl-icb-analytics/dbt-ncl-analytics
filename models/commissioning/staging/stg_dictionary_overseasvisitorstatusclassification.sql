-- Staging model for dictionary.OverseasVisitorStatusClassification
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OverseasVisitorStatusClassificationID" as sk_overseasvisitorstatusclassificationid,
    "BK_OverseasVisitorStatusClassification" as bk_overseasvisitorstatusclassification,
    "OverseasVisitorStatusClassification" as overseasvisitorstatusclassification
from {{ source('dictionary', 'OverseasVisitorStatusClassification') }}
