{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OverseasVisitorStatusClassification \ndbt: source(''dictionary_dbo'', ''OverseasVisitorStatusClassification'') \nColumns:\n  SK_OverseasVisitorStatusClassificationID -> sk_overseas_visitor_status_classification_id\n  BK_OverseasVisitorStatusClassification -> bk_overseas_visitor_status_classification\n  OverseasVisitorStatusClassification -> overseas_visitor_status_classification"
    )
}}
select
    "SK_OverseasVisitorStatusClassificationID" as sk_overseas_visitor_status_classification_id,
    "BK_OverseasVisitorStatusClassification" as bk_overseas_visitor_status_classification,
    "OverseasVisitorStatusClassification" as overseas_visitor_status_classification
from {{ source('dictionary_dbo', 'OverseasVisitorStatusClassification') }}
