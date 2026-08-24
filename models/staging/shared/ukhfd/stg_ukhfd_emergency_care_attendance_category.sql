select
    "Main_Code_Text" as attendance_category_code
    , "Main_Description" as attendance_category_desc
from {{ source('ukhfd_data_dictionary', 'emergency_care_attendance_category') }}
where "Is_Latest" = 1
qualify row_number() over (
    partition by "Main_Code_Text"
    order by "Effective_From" desc nulls last
) = 1
