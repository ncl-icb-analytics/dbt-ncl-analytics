select
    sk_bnfid,
    sk_bnf_parent_id,
    sk_bnf_chapter_id,
    type_num,
    type,
    code,
    name,
    path,
    path_depth,
    is_substance,
    is_product,
    is_presentation,
    date_created,
    date_updated,
    is_generic,
    sk_bnfid_generic_equivalent
from {{ ref('raw_dictionary_dbo_bnf_substance_product_presentation') }}