# Cambridge Multimorbidity Score Overview

## Purpose

The pipeline computes a person-level Cambridge Comorbidity Score (CCMS) for adults aged 16+ to support risk stratification and downstream modelling.
This implementation has been migrated from the London AIC version of CCMS. For migration details, assumptions, and change history, review the pull request.

## Outstanding issues
- There is a dependency on an AIC S3 bucket of defintitions [ staged CCMS codelists (`stg_aic_base_ccms_snomed_codes`, `stg_aic_base_ccms_dmd_codes`)] that should be reviewed.
- Exact model version that the weights are taken from must be confirmed with the AIC centre
- Local unit testing

## Relevant Literature
- Payne RA, et al. (2020). *The Cambridge Multimorbidity Score: a clinically useful tool for predicting mortality in primary care.* [https://doi.org/10.3399/bjgp20X709433](https://doi.org/10.3399/bjgp20X709433)
- Brilleman SL, et al. (2022). *Updating and validating the Cambridge Multimorbidity Score in England.* [https://bjgp.org/content/73/731/e435](https://bjgp.org/content/73/731/e435)

## Model Flow
The pipeline uses staged CCMS codelists (`stg_aic_base_ccms_snomed_codes`, `stg_aic_base_ccms_dmd_codes`) and OLIDS observations/medication data.

1. `int_ccms_medication_orders`
   - Filters medication orders to CCMS dm+d codelists.
   - Applies a 12-month lookback for most medication features, with an all-time exception for lithium.

2. `int_ccms_intermediate_flags`
   - Creates one row per eligible person with raw binary flags from:
     - SNOMED observation signals
     - Medication thresholds
     - eGFR logic (two most recent values, both < 60)
   - Applies CCMS lookback rules (mixed 12-month, 5-year, and all-time logic depending on condition).
   - Filters cohort to >= 16 years

3. `int_ccms_final_flags`
   - Maps raw flags into Cambridge condition groupings used for scoring.
   - Combines related clinical and medication proxies into the final set of score inputs.

4. `int_ccms_score`
   - Applies published weighted coefficients to final flags.
   - Outputs a person-level `cambridge_comorbidity_score`.

5. `dim_person_ccms`
   - Mart for downstream use