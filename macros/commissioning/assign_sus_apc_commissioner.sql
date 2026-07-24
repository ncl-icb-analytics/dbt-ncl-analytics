/*
Assigns commissioner to APC spells using hierarchical lookup:
1. GP practice code from spell admission record
2. Patient LSOA from demographics
3. Provider code (hosted provider scenario)
4. Provider postcode (fallback)

Handles date-effective intervals and overlapping date ranges deterministically.
Returns commissioner code for the spell at admission date.
*/

{% macro assign_sus_apc_commissioner(
    spell_table_ref,
    commissioner_practice_ref,
    commissioner_lsoa_ref,
    commissioner_provider_ref,
    commissioner_provider_postcode_ref
) %}

    with spell_base as (
        select
            {{ spell_table_ref }}.primarykey_id as spell_id,
            {{ spell_table_ref }}.sk_patient_id,
            {{ spell_table_ref }}.spell_admission_date,
            {{ spell_table_ref }}.spell_gp_practice_code,
            {{ spell_table_ref }}.spell_commissioning_service_agreement_provider,
            {{ spell_table_ref }}.patient_lsoa_code,
            {{ spell_table_ref }}.provider_postcode
        from {{ spell_table_ref }}
    ),

    /* Priority 1: GP Practice-based commissioner lookup */
    commissioner_practice_lookup as (
        select
            spell_base.spell_id,
            spell_base.sk_patient_id,
            spell_base.spell_admission_date,
            {{ commissioner_practice_ref }}.organisationcode_commissioner as commissioner_code,
            1 as lookup_priority,
            'PRACTICE' as lookup_method,
            row_number() over (
                partition by spell_base.spell_id 
                order by {{ commissioner_practice_ref }}.organisationcode_commissioner
            ) as rn
        from spell_base
        left join {{ commissioner_practice_ref }}
            on spell_base.spell_gp_practice_code = {{ commissioner_practice_ref }}.organisationcode_practice
            and spell_base.spell_admission_date >= {{ commissioner_practice_ref }}.relationshipstartdate
            and spell_base.spell_admission_date < dateadd(day, 1, {{ commissioner_practice_ref }}.relationshipenddate)
            and {{ commissioner_practice_ref }}.isactive = true
        where spell_base.spell_gp_practice_code is not null
            and {{ commissioner_practice_ref }}.organisationcode_commissioner is not null
    ),

    /* Priority 2: LSOA-based commissioner lookup */
    commissioner_lsoa_lookup as (
        select
            spell_base.spell_id,
            spell_base.sk_patient_id,
            spell_base.spell_admission_date,
            {{ commissioner_lsoa_ref }}.organisationcode_commissioner as commissioner_code,
            2 as lookup_priority,
            'LSOA' as lookup_method,
            row_number() over (
                partition by spell_base.spell_id 
                order by {{ commissioner_lsoa_ref }}.organisationcode_commissioner
            ) as rn
        from spell_base
        left join {{ commissioner_lsoa_ref }}
            on spell_base.patient_lsoa_code = {{ commissioner_lsoa_ref }}.oacode
            and spell_base.spell_admission_date >= {{ commissioner_lsoa_ref }}.effectivefrom
            and spell_base.spell_admission_date < dateadd(day, 1, {{ commissioner_lsoa_ref }}.effectiveto)
        where spell_base.patient_lsoa_code is not null
            and {{ commissioner_lsoa_ref }}.organisationcode_commissioner is not null
    ),

    /* Priority 3: Provider (Hosted Provider) lookup */
    commissioner_provider_lookup as (
        select
            spell_base.spell_id,
            spell_base.sk_patient_id,
            spell_base.spell_admission_date,
            {{ commissioner_provider_ref }}.commissionercode as commissioner_code,
            3 as lookup_priority,
            'PROVIDER' as lookup_method,
            row_number() over (
                partition by spell_base.spell_id 
                order by {{ commissioner_provider_ref }}.commissionercode
            ) as rn
        from spell_base
        left join {{ commissioner_provider_ref }}
            on spell_base.spell_commissioning_service_agreement_provider = {{ commissioner_provider_ref }}.providercode
            and spell_base.spell_admission_date >= {{ commissioner_provider_ref }}.effectivefrom
            and spell_base.spell_admission_date < dateadd(day, 1, {{ commissioner_provider_ref }}.effectiveto)
        where spell_base.spell_commissioning_service_agreement_provider is not null
            and {{ commissioner_provider_ref }}.commissionercode is not null
    ),

    /* Priority 4: Provider Postcode lookup */
    commissioner_provider_postcode_lookup as (
        select
            spell_base.spell_id,
            spell_base.sk_patient_id,
            spell_base.spell_admission_date,
            {{ commissioner_provider_postcode_ref }}.commissionercode as commissioner_code,
            4 as lookup_priority,
            'PROVIDER_POSTCODE' as lookup_method,
            row_number() over (
                partition by spell_base.spell_id 
                order by {{ commissioner_provider_postcode_ref }}.commissionercode
            ) as rn
        from spell_base
        left join {{ commissioner_provider_postcode_ref }}
            on spell_base.spell_commissioning_service_agreement_provider = {{ commissioner_provider_postcode_ref }}.providercode
            and spell_base.provider_postcode = {{ commissioner_provider_postcode_ref }}.postcode
            and spell_base.spell_admission_date >= {{ commissioner_provider_postcode_ref }}.effectivefrom
            and spell_base.spell_admission_date < dateadd(day, 1, {{ commissioner_provider_postcode_ref }}.effectiveto)
        where spell_base.spell_commissioning_service_agreement_provider is not null
            and spell_base.provider_postcode is not null
            and {{ commissioner_provider_postcode_ref }}.commissionercode is not null
    ),

    /* Combine all lookups, retaining first successful priority */
    combined_lookups as (
        select * from commissioner_practice_lookup where rn = 1 and commissioner_code is not null
        union all
        select * from commissioner_lsoa_lookup 
        where rn = 1 
            and commissioner_code is not null
            and spell_id not in (select spell_id from commissioner_practice_lookup where commissioner_code is not null)
        union all
        select * from commissioner_provider_lookup 
        where rn = 1 
            and commissioner_code is not null
            and spell_id not in (select spell_id from commissioner_practice_lookup where commissioner_code is not null)
            and spell_id not in (select spell_id from commissioner_lsoa_lookup where commissioner_code is not null)
        union all
        select * from commissioner_provider_postcode_lookup 
        where rn = 1 
            and commissioner_code is not null
            and spell_id not in (select spell_id from commissioner_practice_lookup where commissioner_code is not null)
            and spell_id not in (select spell_id from commissioner_lsoa_lookup where commissioner_code is not null)
            and spell_id not in (select spell_id from commissioner_provider_lookup where commissioner_code is not null)
    ),

    /* Final result with fallback to unknown */
    final_result as (
        select
            spell_base.spell_id,
            spell_base.sk_patient_id,
            spell_base.spell_admission_date,
            coalesce(combined_lookups.commissioner_code, 'UNKNOWN') as commissioner_code,
            coalesce(combined_lookups.lookup_method, 'NOT_FOUND') as commissioner_lookup_method,
            coalesce(combined_lookups.lookup_priority, 99) as commissioner_lookup_priority
        from spell_base
        left join combined_lookups
            on spell_base.spell_id = combined_lookups.spell_id
    )

    select * from final_result

{% endmacro %}
