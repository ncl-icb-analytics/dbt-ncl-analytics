/*
OLIDS prescribing cost per person per month (NCL only - OLIDS is EMIS NCL).

Uses MEDICATION_ORDER.estimated_cost - EMIS's per-order cost estimate.
Checked May 2025 - Apr 2026: 100% populated, 95.7% positive. Window total
GBP 239m vs GBP 238m when priced at EPD mean actual cost per item by BNF
code (0.4% apart). Basis is an estimate, not reimbursed spend.

orders_costed counts orders with a positive estimate.
*/

{{ config(materialized = 'table') }}

select
    o.person_id,
    date_trunc('month', o.clinical_effective_date)::date as activity_month,
    count(*)                                           as prescription_orders,
    count_if(o.estimated_cost > 0)                     as orders_costed,
    round(sum(coalesce(o.estimated_cost, 0)), 2)       as prescribing_cost_modelled
from {{ ref('stg_olids_medication_order') }} as o
where o.person_id is not null
  and o.clinical_effective_date is not null
group by 1, 2
