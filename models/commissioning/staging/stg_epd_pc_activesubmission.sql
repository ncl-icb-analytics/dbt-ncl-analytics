-- Staging model for epd_primary_care.ActiveSubmission
-- Source: "DATA_LAKE"."EPD_PRIMARY_CARE"
{% if source.get('description') %}
-- Description: Primary care medications and prescribing data
{% endif %}

select
    "UniqSubmissionId" as uniqsubmissionid
from {{ source('epd_primary_care', 'ActiveSubmission') }}
