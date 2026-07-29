{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}
--publish global refresh date for OLIDS data models
select GLOBAL_DATA_REFRESH_DATE 
from MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE
