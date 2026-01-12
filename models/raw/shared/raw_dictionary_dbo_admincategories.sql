{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.AdminCategories \ndbt: source(''dictionary_dbo'', ''AdminCategories'') \nColumns:\n  SK_AdminCategoryID -> sk_admin_category_id\n  BK_AdminCategoryCode -> bk_admin_category_code\n  AdminCategoryName -> admin_category_name\n  AdminCategoryFullName -> admin_category_full_name\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  BK_AdminCategoryCode_TinyInt -> bk_admin_category_code_tiny_int"
    )
}}
select
    "SK_AdminCategoryID" as sk_admin_category_id,
    "BK_AdminCategoryCode" as bk_admin_category_code,
    "AdminCategoryName" as admin_category_name,
    "AdminCategoryFullName" as admin_category_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "BK_AdminCategoryCode_TinyInt" as bk_admin_category_code_tiny_int
from {{ source('dictionary_dbo', 'AdminCategories') }}
