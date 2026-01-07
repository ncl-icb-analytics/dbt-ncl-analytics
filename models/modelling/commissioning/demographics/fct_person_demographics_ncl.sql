select
        --Record information
        pds.sk_patient_id,
        pds.record_registered_end_date is null as current_ncl_person_flag,
        pds.record_person_end_date,

        --Person information
        pds.year_month_of_birth,
        dict_g.gender,
        pds.date_of_death,
        dict_pl.preferred_language,
        dict_ir.interpreter_required,
    
        --Registered information
        (
                --NCL Practice
                dict_pcn.stp_code = 'QMJ' and
                --The GP Practice is open
                dict_gp.end_date is null and
                --No record of death
                pds.date_of_death is null and
                --No record of the record being removed
                pds.registered_reason_for_removal is null and
                --The GP Registration is still active
                pds.record_registered_end_date is null
                
        ) as current_ncl_registered_flag,
        pds.record_registered_start_date,
        pds.record_registered_end_date,
        pds.practice_code,
        dict_gp.organisation_name as practice_name,
        dict_pcn.network_code as pcn_code,
        dict_pcn.network_name as pcn_name,
        dict_pcn.stp_code as icb_code,
        dict_pcn.stp_name as icb_name,
        geo_reg.local_authority_2025_name as registered_borough,
        nb_reg.neighbourhood_code as registered_neighbourhood_code,
        nb_reg.neighbourhood_name as registered_neighbourhood_name,

        --Resident information
        (
                geo.resident_flag = 'NCL' and 
                pds.record_residence_end_date is null and
                pds.date_of_death is null
        ) as current_ncl_resident_flag,
        pds.record_residence_end_date,
        pds.postcode_sector,
        pds.lsoa_21,
        geo.ward_2025_code,
        geo.ward_2025_name,
        geo.local_authority_2025_name as residence_borough,
        nb_res.neighbourhood_code as residence_neighbourhood_code,
        nb_res.neighbourhood_name as residence_neighbourhood_name,
        imd.index_of_multiple_deprivation_decile as residence_imd_decile,
        imd.index_of_multiple_deprivation_score as residence_imd_score,

        --Ethnicity information
        eth.* EXCLUDE (sk_patientid, record_date),

        --Metadata information
        cast(current_timestamp as datetime) as _timestamp
        
from {{ref('int_pds_latest_record')}} pds

left join {{ref('stg_dictionary_dbo_gender')}} as dict_g
on pds.gender_code = dict_g.gender_code

left join dev__modelling.lookup_ncl.preferred_language as dict_pl
on pds.preferred_language_code = dict_pl.code

left join dev__modelling.lookup_ncl.interpreter_required as dict_ir
on pds.interpreter_required = dict_ir.interpreter_required

left join modelling.lookup_ncl.lsoa_2021_ward_2025_local_authority_2025 geo
on pds.lsoa_21 = geo.lsoa_2021_code

left join {{ref('raw_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021_latest')}} nb_res
on pds.lsoa_21 = nb_res.lsoa_2021_code

left join dev__modelling.lookup_ncl.imd25_imd imd
on pds.lsoa_21 = imd.lsoa_code_2021

left join {{ref('stg_dictionary_dbo_organisation')}} dict_gp
on pds.practice_code = dict_gp.organisation_code

left join {{ref('stg_dictionary_dbo_organisationmatrixpracticeview')}} as dict_pcn
on dict_gp.sk_organisation_id = dict_pcn.sk_organisation_id_practice

left join "Dictionary"."dbo"."Postcode" gp_pc
on dict_gp.sk_postcode_id = gp_pc."SK_Postcode_ID"

left join modelling.lookup_ncl.lsoa_2021_ward_2025_local_authority_2025 geo_reg
on gp_pc.lsoa = geo_reg.lsoa_2021_code

left join {{ref('raw_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021_latest')}} nb_reg
on gp_pc.lsoa = nb_reg.lsoa_2021_code

left join dev__modelling.lookup_ncl.ethnicity_national_data_sets eth
ON pds.sk_patient_id = eth.sk_patientid