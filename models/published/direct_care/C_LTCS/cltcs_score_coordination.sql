

with encoding_features as(
    select patient_id,
        area_code,
        wl_total_count,
        wl_provider_count,
        op_att_tot_12mo,
        op_prov_12mo,
        op_spec_12mo,
        gp_att_tot_12mo,
        ae_tot_12mo,
        apc_los_12mo,
        op_oe_ratio,
        case when has_same_tfc_multiple_providers_flag = TRUE then 1 else 0 end as has_same_tfc_multiple_providers_flag_flag,
    from {{ ref('cohort_data') }}
)
select
    patient_id,
    area_code,
    (wl_total_count * wl_provider_count) + op_att_tot_12mo/3 + op_prov_12mo*2 + op_spec_12mo*2 - gp_att_tot_12mo*4 - ae_tot_12mo - apc_los_12mo + op_oe_ratio * 3 + has_same_tfc_multiple_providers_flag_flag * 2 as score_coordination
from encoding_features