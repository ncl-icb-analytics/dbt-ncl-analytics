-- Raw layer model for reference_analyst_managed.model_hospital_theatre_imported
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
