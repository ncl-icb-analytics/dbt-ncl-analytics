-- Staging model for dictionary_dbo.AdminCategories
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AdminCategoryID" as sk_admin_category_id,
    "BK_AdminCategoryCode" as bk_admin_category_code,
    "AdminCategoryName" as admin_category_name,
    "AdminCategoryFullName" as admin_category_full_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "BK_AdminCategoryCode_TinyInt" as bk_admin_category_code_tiny_int
from {{ source('dictionary_dbo', 'AdminCategories') }}
