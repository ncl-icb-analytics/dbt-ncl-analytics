select
    --Record information
        pmi.sk_patient_id,

        --Person information
        pmi.gender_code,
        dict_g.gender,
        pmi.date_of_birth,
        pmi.date_of_death,
        
        --Ethnicity information
        eth.*,

        --Residence information
        pmi.flag_current_ncl_residence,
        pmi.record_residence_start_date,
        geo.lsoa_2021_code,
        geo.lsoa_2021_name,
        geo.ward_2025_code,
        geo.ward_2025_name,
        geo.local_authority_2025_name as residence_borough,
        nb_res.neighbourhood_code as residence_neighbourhood_code,
        nb_res.neighbourhood_name as residence_neighbourhood_name,
        imd.imd25_decile as residence_imd_decile,

        --Registered information
        pmi.flag_current_ncl_registered,
        pmi.record_registered_start_date,
        pmi.practice_code,
        dict_gp.organisation_name as practice_name,
        dict_pcn.network_code as pcn_code,
        dict_pcn.network_name as pcn_name,
        dict_pcn.stp_code as icb_code,
        dict_pcn.stp_name as icb_name,
        ----Note NCL only for fields below----
        case 
            when (icb_code != 'QMJ' and gp_lu.borough is null) then 'Non-NCL Borough'
            when (icb_code = 'QMJ' and dict_gp.end_date is not null) then 'Unknown due to closed practice'
            else coalesce(gp_lu.borough, reg_bor_backup.borough, 'Unknown')
        end as registered_borough,
        nb_reg.neighbourhood_code as registered_neighbourhood_code,
        case
            when (icb_code != 'QMJ' and nb_reg.neighbourhood_code is null) then 'Non-NCL Neighbourhood'
            else coalesce(nb_reg.neighbourhood_name, 'Unknown')
        end as registered_neighbourhood_name,
        --------------------------------------

        --Language information
        dict_pl.preferred_language,
        dict_ir.interpreter_required

from {{ref('int_person_pmi_combined')}} pmi

left join {{ref('stg_dictionary_dbo_gender')}} as dict_g
on pmi.gender_code = dict_g.gender_code

left join {{ref('stg_reference_lookup_ncl_preferred_language')}} as dict_pl
on pmi.preferred_language_code = dict_pl.code

left join {{ref('stg_reference_lookup_ncl_interpreter_required')}} as dict_ir
on pmi.interpreter_required = dict_ir.interpreter_required

left join {{ref('stg_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025')}} geo
on pmi.lsoa21_code = geo.lsoa_2021_code

left join {{ref('stg_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021')}} nb_res
on pmi.lsoa21_code = nb_res.lsoa_2021_code

left join {{ref('stg_reference_lookup_ncl_imd_2025')}} imd
on pmi.lsoa21_code = imd.lsoa_code_2021

left join {{ref('stg_dictionary_dbo_organisation')}} dict_gp
on pmi.practice_code = dict_gp.organisation_code

left join {{ref('stg_dictionary_dbo_organisationmatrixpracticeview')}} as dict_pcn
on dict_gp.sk_organisation_id = dict_pcn.sk_organisation_id_practice

left join {{ref('stg_reference_lookup_ncl_gp_practice')}} gp_lu
on pmi.practice_code = gp_lu.gp_practice_code

left join (
        select distinct pcn_code, borough
        from {{ref('stg_reference_lookup_ncl_gp_practice')}} 
) reg_bor_backup
on gp_lu.pcn_code = reg_bor_backup.pcn_code

left join {{ref('stg_reference_lookup_ncl_ncl_gp_practice_neighbourhood')}} nb_reg
on pmi.practice_code = nb_reg.practice_code

left join (
    select distinct
        bk_ethnicity_code as ethnicity_code,
        ethnicity_desc,
        split_part(ethnicity_desc, ':', 0) as ethnicity,
        ethnicity_desc2 as ethnicity_detail
    from {{ref('stg_dictionary_dbo_ethnicity')}}
) eth
on pmi.ethnicity_code = eth.ethnicity_code