{{
    config(
        description="Raw layer (Fingertips indicator data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.FINGERTIPS.METADATA_INDICATOR \ndbt: source(''reference_fingertips'', ''METADATA_INDICATOR'') \nColumns:\n  Indicator ID -> indicator_id\n  Indicator -> indicator\n  Indicator number -> indicator_number\n  Rationale -> rationale\n  Specific rationale -> specific_rationale\n  Definition -> definition\n  Data source -> data_source\n  Indicator source -> indicator_source\n  Definition of numerator -> definition_of_numerator\n  Source of numerator -> source_of_numerator\n  Definition of denominator -> definition_of_denominator\n  Source of denominator -> source_of_denominator\n  Methodology -> methodology\n  Standard population/values -> standard_population_values\n  Frequency -> frequency\n  Confidence interval details -> confidence_interval_details\n  Disclosure control -> disclosure_control\n  Rounding -> rounding\n  Caveats -> caveats\n  Notes -> notes\n  Impact of COVID-19 -> impact_of_covid_19\n  Copyright -> copyright\n  Data re-use -> data_re_use\n  Links -> links\n  Indicator Content -> indicator_content\n  Simple Name -> simple_name\n  Simple Definition -> simple_definition\n  Unit -> unit\n  Value type -> value_type\n  Year type -> year_type\n  Polarity -> polarity\n  Date updated -> date_updated"
    )
}}
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
