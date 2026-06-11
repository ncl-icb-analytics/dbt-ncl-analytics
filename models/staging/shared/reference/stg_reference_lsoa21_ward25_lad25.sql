select
    lsoa21_cd,
    lsoa21_nm,
    wd25_cd,
    wd25_nm,
    lad25_cd,
    lad25_nm,
    resident_flag
from {{ ref('raw_reference_lsoa21_ward25_lad25') }}
