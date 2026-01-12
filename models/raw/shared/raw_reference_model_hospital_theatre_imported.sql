{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.model_hospital_theatre_imported \ndbt: source(''reference_analyst_managed'', ''model_hospital_theatre_imported'') \nColumns:\n  Compartment -> compartment\n  SubCompartment -> sub_compartment\n  Domain -> domain\n  Metric -> metric\n  ProviderCode -> provider_code\n  ProviderName -> provider_name\n  FilterName -> filter_name\n  ReportingDate -> reporting_date\n  ProviderValue -> provider_value\n  MedianValue -> median_value\n  MeanValue -> mean_value\n  BenchmarkValue -> benchmark_value\n  PeerValue -> peer_value\n  PeerMean -> peer_mean\n  MinValue -> min_value\n  MaxValue -> max_value\n  Numerator Description -> numerator_description\n  Num: ProviderValue -> num:_provider_value\n  Num: MedianValue -> num:_median_value\n  Denominator Description -> denominator_description\n  Denom: ProviderValue -> denom:_provider_value\n  Denom: MedianValue -> denom:_median_value"
    )
}}
select
    "Compartment" as compartment,
    "SubCompartment" as sub_compartment,
    "Domain" as domain,
    "Metric" as metric,
    "ProviderCode" as provider_code,
    "ProviderName" as provider_name,
    "FilterName" as filter_name,
    "ReportingDate" as reporting_date,
    "ProviderValue" as provider_value,
    "MedianValue" as median_value,
    "MeanValue" as mean_value,
    "BenchmarkValue" as benchmark_value,
    "PeerValue" as peer_value,
    "PeerMean" as peer_mean,
    "MinValue" as min_value,
    "MaxValue" as max_value,
    "Numerator Description" as numerator_description,
    "Num: ProviderValue" as num:_provider_value,
    "Num: MedianValue" as num:_median_value,
    "Denominator Description" as denominator_description,
    "Denom: ProviderValue" as denom:_provider_value,
    "Denom: MedianValue" as denom:_median_value
from {{ source('reference_analyst_managed', 'model_hospital_theatre_imported') }}
