-- Raw layer model for reference_fingertips.METADATA_INDICATOR
-- Source: "DATA_LAKE__NCL"."FINGERTIPS"
-- Description: Fingertips indicator data
-- This is a 1:1 passthrough from source with standardized column names
select
    "Indicator ID" as indicator_id,
    "Indicator" as indicator,
    "Indicator number" as indicator_number,
    "Rationale" as rationale,
    "Specific rationale" as specific_rationale,
    "Definition" as definition,
    "Data source" as data_source,
    "Indicator source" as indicator_source,
    "Definition of numerator" as definition_of_numerator,
    "Source of numerator" as source_of_numerator,
    "Definition of denominator" as definition_of_denominator,
    "Source of denominator" as source_of_denominator,
    "Methodology" as methodology,
    "Standard population/values" as standard_population_values,
    "Frequency" as frequency,
    "Confidence interval details" as confidence_interval_details,
    "Disclosure control" as disclosure_control,
    "Rounding" as rounding,
    "Caveats" as caveats,
    "Notes" as notes,
    "Impact of COVID-19" as impact_of_covid_19,
    "Copyright" as copyright,
    "Data re-use" as data_re_use,
    "Links" as links,
    "Indicator Content" as indicator_content,
    "Simple Name" as simple_name,
    "Simple Definition" as simple_definition,
    "Unit" as unit,
    "Value type" as value_type,
    "Year type" as year_type,
    "Polarity" as polarity,
    "Date updated" as date_updated
from {{ source('reference_fingertips', 'METADATA_INDICATOR') }}
