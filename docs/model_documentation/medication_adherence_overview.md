# Medicines Adherence (PDC) Overview

## Purpose

The pipeline computes rolling and overall Proportion of Days Covered (PDC) — the standard prescribing-based medication adherence measure — per person and drug (VTM grain) across five drug classes: RAAS, beta-blockers, calcium-channel blockers, non-insulin anti-diabetics and lipid lowering drugs (BNF paragraphs 020505, 020400, 020602, 060102, 021200).
This implementation is a faithful port of the AIC centre meds_adherence Snowpark pipeline deployed in NEL and SEL (`MedicationTableSnowpark`, `compute_pdc_rolling` with a 12-month rolling window, dynamic exposure denominator `pdc_type=2` and exclusive overlap counting) into WNL native dbt models. Every deliberate divergence from the original is documented in the model headers; alongside the faithful `pdc` / `overall_pdc` columns, `*_corrected` variants fix the original's last-order quirk (see below) so the two can be compared before choosing which to consume.

## Outstanding issues
- **Results not yet validated**: the logic is complete and builds, but `analyses/medication_adherence_pdc_validation.sql` has not been run through — sections 1–3 (VTM resolution coverage, reference ambiguity, chain fragmentation) are the decision gates.
- **Last-order quirk decision**: the original 0-fills `days_to_next_order` on the final order of each refill chain, so that order contributes ~0 covered days. Both faithful and corrected columns are emitted; which is authoritative for reporting is undecided pending validation.
- **No numeric parity check is possible**: tNo comparator data in WNL environment to validate against but have the ISPORE poster to replicate validation
- **VTM resolution reliability**: refill chains group on the VTM code resolved via `stg_reference_bnf_latest` (SNOMED tier, then 9-char BNF chemical-substance tier, then concept-display fallback). This does not have an automated refrash
- **Selection nuance**: the original filtered on its environment's `bnf_reference`; this port filters on the repo's canonical dm+d-derived `bnf_code` via `get_medication_orders`.
- Local unit testing of the dose-instruction parser (the frequency pattern precedence is replicated verbatim but only eyeball-validated via the analysis queries).

## Relevant Literature
- Prieto-Merino D, Mulick A, Armstrong C, Hoult H, Fawcett S, Eliasson L, et al. Estimating proportion of days covered (PDC) using real-world online medicine suppliers’ datasets. Journal of Pharmaceutical Policy and Practice. 2021;14.  [Open access URL](https://pubmed.ncbi.nlm.nih.gov/34965882/)
- Dalli LL, Kilkenny MF, Arnet I, Sanfilippo FM, Cummings DM, Kapral MK, et al. Towards better reporting of the proportion of days covered method in cardiovascular medication adherence: A scoping review and new tool TEN-SPIDERS. British Journal of Clinical Pharmacology. 2022;88(10):4427–42.  [Open access URL](https://pmc.ncbi.nlm.nih.gov/articles/PMC9546055/)
- PDC as the preferred adherence measure: Pharmacy Quality Alliance (PQA) adherence measure specifications [URL](https://www.pqaalliance.org/adherence-measures)
- Terminology and definitions for database adherence research: Raebel et al., (2013) *Standardizing terminology and definitions of medication adherence and persistence in research employing electronic databases*, Medical Care [Open access URL](https://pmc.ncbi.nlm.nih.gov/articles/PMC3727405/)

## Model Flow
The pipeline uses OLIDS medication orders (via the `get_medication_orders` macro) and the BNF/dm+d reference (`stg_reference_bnf_latest`) for VTM resolution.

1. `int_medication_adherence_orders`
   - Selects orders in the five BNF paragraphs and labels `drug_class`.
   - Resolves `drug_name` to the VTM code (SNOMED tier → 9-char BNF chemical-substance tier → concept-display fallback, with a `drug_name_source` audit column).
   - Normalises missing-value sentinel strings to NULL and filters to oral solid forms (tablet/capsule quantity units).

2. `int_medication_adherence_order_durations`
   - Parses free-text dose instructions to `tablets_per_day` (ordered regex precedence, replicated verbatim from the original — do not reorder).
   - Derives `calculated_duration` (quantity / tablets-per-day, falling back to recorded `duration_days`) and `order_enddate`.
   - Deduplicates same-day orders keeping the highest quantity; computes `days_to_next_order` per (person, drug) chain in faithful (0-filled) and corrected (NULL on last order) variants.

3. `int_medication_adherence_pdc`
   - Generates person-anchored monthly rolling 12-month windows from each chain's first order.
   - Computes windowed PDC with the dynamic exposure denominator and exclusive overlap subtraction, plus an overall chain PDC — each in faithful and corrected variants.
   - Empty windows are retained with NULL exposure/PDC, matching the original.

4. `fct_person_medication_adherence`
   - Mart for downstream use: joins `dim_person`, re-presents `drug_name` as the readable VTM name via a deduplicated 1:1 code→name map (the grain key remains `vtm_code`).
   - No opt-out filter; secondary-use consumers join `DIM_PERSON_SECONDARY_USE_ALLOWED`.

Validation queries: `analyses/medication_adherence_pdc_validation.sql` (resolution coverage, reference ambiguity, fragmentation, funnel, dose-parse eyeball, PDC distributions, quirk sizing, single-person trace).
