select
    sk_bnf_chapter_id,
    sk_bnf_chapter_parent_id,
    chapter_code,
    chapter_code_alt,
    chapter_code_alt_pad,
    chapter_name,
    chapter_path,
    chapter_path_depth,
    is_official_bnf,
    url,
    date_created,
    date_updated
from {{ ref('raw_dictionary_dbo_bnf_chapter') }}