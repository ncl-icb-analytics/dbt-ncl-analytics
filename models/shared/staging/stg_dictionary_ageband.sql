-- Staging model for dictionary.AgeBand
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AgeBandID" as sk_agebandid,
    "BK_AgeBand" as bk_ageband,
    "AgeBandStarts" as agebandstarts,
    "AgeBandEnds" as agebandends,
    "CreatedDateTime" as createddatetime,
    "LastUpdateDateTime" as lastupdatedatetime
from {{ source('dictionary', 'AgeBand') }}
