{# NHS Census 2001 EthnicCategoryCode → 17+1 category label.
   Pure dictionary lookup — caller must supply a clean letter code (A-Z, 99, 0).
   Returns NULL for codes outside the standard set.

   Source: Dictionary.dbo.EthnicityCode (EthnicityCodeType='EthnicCategoryCode'). #}
{% macro nhs_ethnicity_17_label(code_col) %}
    case upper(trim({{ code_col }}))
        when 'A'  then 'White: British'
        when 'B'  then 'White: Irish'
        when 'C'  then 'White: Any other White background'
        when 'D'  then 'Mixed: White and Black Caribbean'
        when 'E'  then 'Mixed: White and Black African'
        when 'F'  then 'Mixed: White and Asian'
        when 'G'  then 'Mixed: Any other Mixed background'
        when 'H'  then 'Asian or Asian British: Indian'
        when 'J'  then 'Asian or Asian British: Pakistani'
        when 'K'  then 'Asian or Asian British: Bangladeshi'
        when 'L'  then 'Asian or Asian British: Any other Asian background'
        when 'M'  then 'Black or Black British: Caribbean'
        when 'N'  then 'Black or Black British: African'
        when 'P'  then 'Black or Black British: Any other Black background'
        when 'R'  then 'Other Ethnic Groups: Chinese'
        when 'S'  then 'Other Ethnic Groups: Any other ethnic group'
        when 'Z'  then 'Not stated'
        when '99' then 'Not known'
        when '0'  then 'Not known'
    end
{% endmacro %}
