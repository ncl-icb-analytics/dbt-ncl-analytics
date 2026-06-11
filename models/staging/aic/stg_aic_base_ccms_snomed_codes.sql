select
    conditionid,
    conditionname,
    conceptid,
    primaryterm
from {{ ref('raw_aic_base_ccms_snomed_codes') }}
