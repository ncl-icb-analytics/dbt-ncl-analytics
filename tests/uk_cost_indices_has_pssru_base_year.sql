-- Asserts the uk_cost_indices seed contains the PSSRU base fiscal year
-- (2023-24, fiscal_year_start = 2023). int_appointment_gp_clean does a
-- CROSS JOIN against this row to compute the GDP deflator ratio for
-- contemporaneous appointment costing — if the row is missing the join
-- would silently return zero rows and empty the entire model.
--
-- The test fails when the SELECT returns rows. We invert the predicate
-- so it returns rows ONLY when the 2023 deflator is missing.

select 1 as missing_pssru_base_year
where not exists (
    select 1
    from {{ ref('uk_cost_indices') }}
    where fiscal_year_start = 2023
      and gdp_deflator is not null
)
