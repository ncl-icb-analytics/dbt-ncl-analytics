{{
    config(
        materialized='table')
}}


/*
All Recent referrals outcomes

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

with base as (
    select
        UBRN_ID,
        action_dt_tm,
        action_desc,
        from {{ ref('stg_ers_pc_ebsx02ubrnaction')}}
        where  E_REFERRAL_PATHWAY_START BETWEEN DATEADD(YEAR, -2, CURRENT_DATE()) AND CURRENT_DATE()
            and E_REFERRAL_PATHWAY_START<=CURRENT_DATE()
)

select
    UBRN_ID,
    listagg(action_desc, '> ') within group (order by action_dt_tm asc) as ordered_actions
from base
group by UBRN_ID