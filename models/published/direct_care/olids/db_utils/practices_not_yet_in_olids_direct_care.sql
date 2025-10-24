{{
    config(
        materialized='view',
        alias='practices_not_yet_in_olids',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
Practices Missing from OLIDS (Direct Care)

Published view showing practices in the reference lookup with no registered
patients in OLIDS demographics.

Use Cases:
- Practice coverage monitoring
- Data feed troubleshooting
- Onboarding validation for new practices
- Regular data completeness checks

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Use for data quality dashboards
- Track count over time to monitor improvements
*/

SELECT
    practice_code,
    practice_name,
    local_authority,
    practice_neighbourhood
FROM {{ ref('int_practices_missing_from_olids') }}
ORDER BY practice_name
