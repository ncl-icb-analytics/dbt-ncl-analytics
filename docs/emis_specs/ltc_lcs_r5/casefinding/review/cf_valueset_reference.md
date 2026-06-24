# CF valueset ground-truth reference

Built from live DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_VALUESETS (341 valuesets, 178 CF reports).
Macros resolve a token by valueset_id OR upper(friendly_name). Where a friendly name is
AMBIGUOUS (same name, different code sets across headline vs _woEX report), pass the
valueset_id (UUID), not the name, else the macro unions both sets.

## Collision valuesets -- PIN THE valueset_id (20)

The headline ICB_CF_<cond>_<n> report carries the small Rule-1 MARKER (exp<=1, e.g.
"X excluded/confirmed/resolved"); the _woEX report carries the big EXCLUSION/CRITERIA set.

| friendly_name | purpose | valueset_id | hash | exp/orig | example |
|---|---|---|---|---|---|
| `af_case_finding_eligible_population_on_af_medication_vs1` | Rule-1 marker (headline) | `9d65ae05-99d1-8995-36b4-d64960af37e5` | ea9dcb07 | 1/2 | ICB_CF_AF_61 |
| `af_case_finding_eligible_population_on_af_medication_vs1` | woEX exclusion/criteria set | `f4f774b1-0949-b062-b74a-d03f7d820a81` | 1dd431f7 | 806/223 | ICB_CF_AF_61_base_woEX |
| `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` | Rule-1 marker (headline) | `5bceb829-7789-d579-1163-85c6908516e6` | ea9dcb07 | 1/2 | ICB_CF_AF_62 |
| `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` | woEX exclusion/criteria set | `c73a0da9-a1dd-594d-7f07-fac41d07cb80` | d70a795a | 23/10 | ICB_CF_AF_62_woEX |
| `asthma_casefinding_eligible_patients_vs1` | Rule-1 marker (headline) | `0bda5482-083e-41ba-a5eb-da032dd51b16` | 95d46c1f | 1/1 | ICB_CF_CYPAST_61 |
| `asthma_casefinding_eligible_patients_vs1` | woEX exclusion/criteria set | `54940f0f-b140-2393-9d8d-0104b3103367` | 52319d3b | 469/469 | ICB_CF_CYPAST_61_woEX |
| `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` | Rule-1 marker (headline) | `7b63b8f0-5846-9636-b5b0-7c2e2f177f30` | b8a16fba | 1/1 | ICB_CF_CKD_64_1D |
| `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` | woEX exclusion/criteria set | `4fad1128-518d-aab7-8bb8-ed51bd35c7b6` | 0e38ef78 | 66/11 | ICB_CF_CKD_64_1D_woEX |
| `gestational_dm_and_no_hba1c_in_the_last_year_vs1` | Rule-1 marker (headline) | `5d6cf960-e4cd-9015-fd93-5105d7bf219a` | 29adf77b | 0/1 | ICB_CF_DM_62 |
| `gestational_dm_and_no_hba1c_in_the_last_year_vs1` | woEX exclusion/criteria set | `56fd73ba-981e-cab3-0404-8c26b02afb36` | 95d9e41a | 3/3 | ICB_CF_DM_62_woEX |
| `hf_case_finding_eligible_patients_vs1` | woEX exclusion/criteria set | `9228a15e-e6cb-1dd2-6074-0c306c800c1b` | 15a4071a | 3/1 | ICB_CF_HF_61 |
| `hf_case_finding_eligible_patients_vs1` | woEX exclusion/criteria set | `299f9e3b-bbab-c65e-c19e-9c531f91755a` | 91ae3dcc | 268/2 | ICB_CF_HF_61_woEX |
| `latest_hba1c_42_45_and_no_hba1c_in_the_last_year_vs1` | Rule-1 marker (headline) | `343c45e1-2edb-f252-16d1-d6e1955659ea` | 29adf77b | 0/1 | ICB_CF_DM_66 |
| `latest_hba1c_42_45_and_no_hba1c_in_the_last_year_vs1` | woEX exclusion/criteria set | `a4dfc336-6ec7-4cec-21fa-c7d3e7842f2d` | 95d9e41a | 3/3 | ICB_CF_DM_66_woEX |
| `latest_hba1c_46_47_and_no_hba1c_in_the_last_year_vs1` | Rule-1 marker (headline) | `776f1587-24f9-1d71-a7ff-6ff4e2a1c27d` | 29adf77b | 0/1 | ICB_CF_DM_63 |
| `latest_hba1c_46_47_and_no_hba1c_in_the_last_year_vs1` | woEX exclusion/criteria set | `160ec384-bb4f-915b-9a8e-66d118eefe0c` | 95d9e41a | 3/3 | ICB_CF_DM_63_woEX |
| `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` | Rule-1 marker (headline) | `877b18f5-9756-23a8-7a41-1900d12e70fa` | 29adf77b | 0/1 | ICB_CF_DM_61 |
| `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` | woEX exclusion/criteria set | `b7ce1049-b4c0-1477-ec22-7696854f03ed` | 18df1b4c | 7/7 | ICB_CF_DM_61_woEX |
| `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` | Rule-1 marker (headline) | `685fbf55-eda4-0f70-9bfc-3a0b55508944` | c0e1227a | 1/1 | ICB_CF_CVD_61 |
| `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` | woEX exclusion/criteria set | `89e5bfaa-b61e-92e0-8e4d-00d41f703b64` | 7b7df77c | 6/1 | ICB_CF_CVD_61_woEX |
| `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1` | Rule-1 marker (headline) | `a318069f-408c-4436-fbaa-070018f2db3a` | c0e1227a | 1/1 | ICB_CF_CVD_63 |
| `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1` | woEX exclusion/criteria set | `23f72268-d2d1-6fb0-9fe5-19eeb497790e` | 561cf433 | 2/4 | ICB_CF_CVD_63_woEX |
| `uclp_pg1_highest_risk_vs1` | Rule-1 marker (headline) | `166bb252-61af-1b31-8ebc-e651bbbd4f29` | a4b6291a | 1/1 | ICB_CF_HTN_61 |
| `uclp_pg1_highest_risk_vs1` | woEX exclusion/criteria set | `9f24e551-90ff-610d-61e7-ca822a831953` | 9313261b | 16/22 | ICB_CF_HTN_61_woEX |
| `uclp_priority_group_2a_vs1` | Rule-1 marker (headline) | `80f7370c-2495-0ff5-2c56-92fe5fd37f42` | a4b6291a | 1/1 | ICB_CF_HTN_62 |
| `uclp_priority_group_2a_vs1` | woEX exclusion/criteria set | `dfa19d4f-2d74-deab-3a99-ab0ea818a3c5` | 9313261b | 16/22 | ICB_CF_HTN_62_woEX |
| `uclp_priority_group_2b_vs1` | Rule-1 marker (headline) | `1e093c88-90aa-0c4e-adf1-e0a8daef3b5d` | a4b6291a | 1/1 | ICB_CF_HTN_63 |
| `uclp_priority_group_2b_vs1` | woEX exclusion/criteria set | `feda64d2-aba7-a4ce-0365-8e7f0237d461` | 88a4167f | 74/78 | ICB_CF_HTN_63_woEX |
| `uclp_priority_group_3a_vs1` | Rule-1 marker (headline) | `590709e2-0364-85a8-2fe6-ebf8af31cddb` | a4b6291a | 1/1 | ICB_CF_HTN_65 |
| `uclp_priority_group_3a_vs1` | woEX exclusion/criteria set | `4df8a74f-daed-fda0-5a8d-81d6f775ab83` | 9313261b | 16/22 | ICB_CF_HTN_65_woEX |
| `uclp_priority_group_3b_vs1` | Rule-1 marker (headline) | `a1a449b9-c2c3-792f-b08d-0142c3de3744` | a4b6291a | 1/1 | ICB_CF_HTN_66 |
| `uclp_priority_group_3b_vs1` | woEX exclusion/criteria set | `8c607c29-9273-b754-5056-727bbbc8d5fa` | 9313261b | 16/22 | ICB_CF_HTN_66_woEX |
| `with_a_qrisk2_10_and_not_on_a_statin_vs1` | Rule-1 marker (headline) | `20072ac6-a53f-00e5-6e47-a40a38a59995` | c0e1227a | 1/1 | ICB_CF_CVD_64 |
| `with_a_qrisk2_10_and_not_on_a_statin_vs1` | woEX exclusion/criteria set | `73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4` | 2ab5ff0c | 845/1 | ICB_CF_CVD_64_woEX |
| `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` | Rule-1 marker (headline) | `4ed7513e-8c5c-68c8-3850-f4bff1387f79` | 29adf77b | 0/1 | ICB_CF_DM_64 |
| `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` | woEX exclusion/criteria set | `5bf8f69d-5b7e-b917-f3bf-4dcda11c8b6f` | 95d9e41a | 3/3 | ICB_CF_DM_64_woEX |
| `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` | Rule-1 marker (headline) | `6e168908-5577-8967-f124-d109c66126e1` | b8a16fba | 1/1 | ICB_CF_CKD_61_1D |
| `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` | woEX exclusion/criteria set | `cc380a59-cc4c-ee42-4a2e-30ab9286dc71` | da882bb6 | 8/8 | ICB_CF_CKD_61_1D_woEX |
| `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` | Rule-1 marker (headline) | `e291831b-466c-ed7a-bba4-eeef6449a8f6` | c0e1227a | 1/1 | ICB_CF_CVD_65 |
| `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` | woEX exclusion/criteria set | `ed40f2e7-f9f3-1a17-104a-42af018aaf32` | c985e0b5 | 16/19 | ICB_CF_CVD_65_woEX |

## Per-report valuesets (all 178 CF reports)


### CF_NHSHC2Y
- `had_nhs_health_check_with_in_last_2_years_vs1` id=`b4840b00-a8f1-d4e3-4b05-12b4eebfeebc` (7/9 codes)

### ICB_CF_AF_61
- `af_case_finding_eligible_population_on_af_medication_vs1` id=`9d65ae05-99d1-8995-36b4-d64960af37e5` (1/2 codes) **[COLLISION->use id]**

### ICB_CF_AF_61_BASE
- `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs1` id=`364cfb51-0257-bb5b-6574-20d108d8c083` (0/1 codes)
- `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs2` id=`98b8e1d3-7db0-0653-9412-2fdf795e6f61` (296/3 codes)
- `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs3` id=`18490fed-2975-d2ea-fcc3-14479e68182a` (0/1 codes)
- `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs4` id=`12984d5f-a198-26f9-6adc-ff4a2e391521` (0/1 codes)

### ICB_CF_AF_61_base_woEX
- `af_case_finding_eligible_population_on_af_medication_vs1` id=`f4f774b1-0949-b062-b74a-d03f7d820a81` (806/223 codes) **[COLLISION->use id]**
- `af_case_finding_eligible_population_on_af_medication_vs2` id=`08ba16a7-dfb1-1282-f9a6-288f2641f431` (31/8 codes)
- `af_case_finding_eligible_population_on_af_medication_vs3` id=`c7ea5a27-039f-d39d-bfbb-5a5c37240961` (1/1 codes)
- `af_case_finding_eligible_population_on_af_medication_vs4` id=`8463a4a4-8199-6956-5458-a3014aaa8f11` (1/1 codes)

### ICB_CF_AF_62
- `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` id=`5bceb829-7789-d579-1163-85c6908516e6` (1/2 codes) **[COLLISION->use id]**

### ICB_CF_AF_62_woEX
- `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` id=`c73a0da9-a1dd-594d-7f07-fac41d07cb80` (23/10 codes) **[COLLISION->use id]**
- `af_case_finding_eligible_population_over_65s_missing_pulse_vs2` id=`9dababfb-2024-76b0-54e2-ec88e66a761d` (13/2 codes)
- `af_case_finding_eligible_population_over_65s_missing_pulse_vs3` id=`505d2779-d0b2-d02d-fa32-f4b8a76db981` (5/8 codes)
- `af_case_finding_eligible_population_over_65s_missing_pulse_vs4` id=`8222cbd6-3f39-5069-d98e-f43179a126fe` (52/28 codes)

### ICB_CF_AF_BP_62
- `requires_bp_check_vs1` id=`e2801f3d-1a7a-0475-9837-d1057df4544c` (17/1 codes)

### ICB_CF_AF_NHSHC_62_1N
- `requires_nhs_health_check_vs1` id=`e422a824-b59b-a0d5-52e2-ff7163a20636` (1/1 codes)

### ICB_CF_AF_PULIRR_62
- `pulse_irregular_requires_review_of_af_coding_vs1` id=`642ec289-c888-9014-8545-4db655538b4b` (5/2 codes)

### ICB_CF_AF_PULSE_62
- `requires_pulse_check_vs1` id=`33f365d1-7d5c-c892-f569-fece3429b7c9` (13/2 codes)

### ICB_CF_CKD_61_1D
- `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` id=`6e168908-5577-8967-f124-d109c66126e1` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CKD_61_1D_woEX
- `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` id=`cc380a59-cc4c-ee42-4a2e-30ab9286dc71` (8/8 codes) **[COLLISION->use id]**
- `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs2` id=`c9896ca5-15c8-0009-a57a-bd6f5067dcad` (8/6 codes)

### ICB_CF_CKD_61_BASE
- `eligible_for_ckd_case_finding_vs1` id=`d83d89d4-dfed-6717-12ce-15a4002c9167` (82/162 codes)

### ICB_CF_CKD_62
- `with_most_recent_2_uacr4_and_not_on_ckd_regckd_diagnosis_vs1` id=`1daf7ba0-0a74-840c-3af3-83affee70a7f` (1/1 codes)

### ICB_CF_CKD_62_woEX
- `with_most_recent_2_uacr4_and_not_on_ckd_reg_vs1` id=`c75746ce-5d7b-1647-f08a-ef5b10445f4c` (2/1 codes)

### ICB_CF_CKD_63_1D
- `with_most_recent_uacr70_and_not_on_ckd_regckd_diagnosi_vs1` id=`21ca7a93-5261-c6d6-0e7d-29f497aeb753` (1/1 codes)

### ICB_CF_CKD_63_1D_woEX
- `with_most_recent_uacr70_and_not_on_ckd_reg_vs1` id=`6bfbf92c-3c89-cc89-f541-a7dcaa945562` (2/1 codes)

### ICB_CF_CKD_64_1D
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` id=`7b63b8f0-5846-9636-b5b0-7c2e2f177f30` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CKD_64_1D_woEX
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` id=`4fad1128-518d-aab7-8bb8-ed51bd35c7b6` (66/11 codes) **[COLLISION->use id]**
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs2` id=`a30e0c52-efd9-9ce9-c56a-6d1469fc64d4` (278/28 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs3` id=`fbe2ad31-b56c-aa50-8270-4825ce03b479` (0/1 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs4` id=`7d11f002-bdf4-e22d-4aa4-5e1de14bb3af` (47/2 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs5` id=`224ba3e2-8d1a-601a-803a-641b8f3c0f28` (0/1 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs6` id=`67b57e4a-b4c4-f213-726c-bcead7df9e1a` (224/2 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs7` id=`37f2f047-d4b2-bf56-5c7b-2a9788936448` (7/5 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs8` id=`63bf0d9b-589c-905a-abd2-e59190e02053` (2/2 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs9` id=`5bf47b55-ccf0-9e25-d7d1-0a7f4b5f3dac` (6/4 codes)
- `at_risk_of_ckd_who_are_not_on_ckd_reg_vs10` id=`3b7a55b7-b9a6-f23b-60d6-1015949622d2` (2/1 codes)

### ICB_CF_CKD_64_BASE
- `ckd_case_finding_vs1` id=`a491afe2-cbfe-8024-0c9b-09749af1c569` (8/7 codes)

### ICB_CF_CKD_BP_62
- `requires_bp_check_vs1` id=`e4d6bb0e-c583-0257-aa39-7b25978da77f` (17/1 codes)

### ICB_CF_CKD_UACR_62
- `requires_uacr_check_vs1` id=`d4bd21b8-1d2d-2bea-e9a1-151905ef2820` (2/1 codes)

### ICB_CF_CKD_eGFR_62
- `requires_egfr_check_vs1` id=`d9757946-4fe7-22ce-c6e6-d50c4fba4f06` (8/6 codes)

### ICB_CF_CVD_61
- `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` id=`685fbf55-eda4-0f70-9bfc-3a0b55508944` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CVD_61_BASE
- `people_with_aged_40_84_with_qrisk_recorded_vs1` id=`476ee994-f9a6-21f8-16ac-bcd937a23da5` (91/91 codes)
- `people_with_aged_40_84_with_qrisk_recorded_vs2` id=`64531256-e997-1a46-caf9-45de4bd95bbe` (845/1 codes)
- `people_with_aged_40_84_with_qrisk_recorded_vs3` id=`38fdc3b5-7de4-b15c-6577-3f3142bb313e` (8/52 codes)
- `people_with_aged_40_84_with_qrisk_recorded_vs4` id=`312c880f-3b26-2eef-48ff-7f2795ed33ef` (3/3 codes)
- `people_with_aged_40_84_with_qrisk_recorded_vs5` id=`49465e07-b9f2-275e-293a-61c00c62a4a0` (1/1 codes)

### ICB_CF_CVD_61_woEX
- `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` id=`89e5bfaa-b61e-92e0-8e4d-00d41f703b64` (6/1 codes) **[COLLISION->use id]**

### ICB_CF_CVD_62
- `people_at_high_risk_cvd_eligible_population_with_qrisk_15_20_vs1` id=`68d3e80c-08d3-7134-0eb7-4b53b0a59b4b` (1/1 codes)

### ICB_CF_CVD_62_woEX
- `people_at_high_risk_cvd_eligible_population_with_qr15_20_vs1` id=`61b18223-5ea9-7e25-e497-f81bde391db3` (6/1 codes)

### ICB_CF_CVD_63
- `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1` id=`a318069f-408c-4436-fbaa-070018f2db3a` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CVD_63_BASE
- `people_with_qrisk_10_vs1` id=`a06baf8e-f9c8-30ab-cee1-c2ba3af35902` (8/52 codes)
- `people_with_qrisk_10_vs2` id=`cd58c9a2-0e17-f7fe-30d3-effd2a7ec9aa` (3/3 codes)
- `people_with_qrisk_10_vs3` id=`bc214a50-595d-7b11-b391-6f3d8ef2aa96` (6/1 codes)

### ICB_CF_CVD_63_woEX
- `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1` id=`23f72268-d2d1-6fb0-9fe5-19eeb497790e` (2/4 codes) **[COLLISION->use id]**
- `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs2` id=`ebb22ff5-788c-4989-66a7-aaf7a68ebb62` (91/91 codes)
- `qrisk2_10_on_a_statin_and_a_non_hdl_25_vs3` id=`624085a5-e1b3-e962-99f1-e7a6270a20e5` (845/1 codes)

### ICB_CF_CVD_64
- `with_a_qrisk2_10_and_not_on_a_statin_vs1` id=`20072ac6-a53f-00e5-6e47-a40a38a59995` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CVD_64_woEX
- `with_a_qrisk2_10_and_not_on_a_statin_vs1` id=`73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4` (845/1 codes) **[COLLISION->use id]**
- `with_a_qrisk2_10_and_not_on_a_statin_vs2` id=`4ff4b3d4-c379-00dd-3603-bcdb169589c0` (1/1 codes)

### ICB_CF_CVD_65
- `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` id=`e291831b-466c-ed7a-bba4-eeef6449a8f6` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CVD_65_woEX
- `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` id=`ed40f2e7-f9f3-1a17-104a-42af018aaf32` (16/19 codes) **[COLLISION->use id]**
- `with_qrisk210_amd_not_on_a_high_intensity_statin_vs2` id=`fdead4de-c567-b1c4-15af-7e6a88bfb587` (6/1 codes)
- `with_qrisk210_amd_not_on_a_high_intensity_statin_vs3` id=`21e3808c-e538-c84d-ad57-ed87efd5ef46` (577/5 codes)

### ICB_CF_CVD_66
- `aged_75_84_years_who_are_not_on_a_statin_and_no_qrisk_in_l5y_vs1` id=`ae5b5d5d-a74a-a1b1-38db-071db479f3ca` (1/1 codes)

### ICB_CF_CVD_66_BASE
- `people_with_aged_70_84_with_qrisk_recorded_vs1` id=`6cf0e193-860f-4b76-9d06-09a3037d09eb` (845/1 codes)
- `people_with_aged_70_84_with_qrisk_recorded_vs2` id=`75140610-d834-c931-9efa-3f47eaf12f5a` (8/52 codes)
- `people_with_aged_70_84_with_qrisk_recorded_vs3` id=`4b0f96a0-5505-e71e-1db3-09f5f557ce99` (3/3 codes)
- `people_with_aged_70_84_with_qrisk_recorded_vs4` id=`c2aa42c9-1dc5-ef51-69f1-80caa1d29cd5` (1/1 codes)

### ICB_CF_CVD_66_woEX
- `aged_75_84_years_who_are_not_on_a_statin_and_no_qriskin_l5y_vs1` id=`a1c6f038-fa5a-8265-93aa-1a1eecf71007` (6/1 codes)

### ICB_CF_CVD_NHSHC_61_1N
- `requires_nhs_health_check_vs1` id=`74a05023-1271-7cad-1d25-a44b9498ea8b` (1/1 codes)

### ICB_CF_CVD_NHSHC_62_1N
- `requires_nhs_health_check_vs1` id=`21350540-5117-3f25-ff13-9a252392231d` (1/1 codes)

### ICB_CF_CVD_NHSHC_63_1N
- `requires_nhs_health_check_vs1` id=`06b2c429-f4df-c0ad-a30b-ace46b211c83` (1/1 codes)

### ICB_CF_CVD_NHSHC_64_1N
- `requires_nhs_health_check_vs1` id=`591c8a3b-56db-3779-91f9-6769603a96b4` (1/1 codes)

### ICB_CF_CVD_NHSHC_65_1N
- `requires_nhs_health_check_vs1` id=`177246c1-52db-4610-5bf8-e389d6b334fc` (1/1 codes)

### ICB_CF_CVD_NHSHC_66_1N
- `requires_nhs_health_check_vs1` id=`45c7a297-e3c2-6962-b6d1-abdc8f871234` (1/1 codes)

### ICB_CF_CVD_NHSHC_BMI_61_2
- `requires_bmi_check_vs1` id=`a27d6b40-dd40-0301-c3cd-885d500606bd` (7/1 codes)

### ICB_CF_CVD_NHSHC_BMI_62_2
- `requires_bmi_check_vs1` id=`98e09f14-0f32-5de4-8322-83e2674cf403` (7/1 codes)

### ICB_CF_CVD_NHSHC_BMI_63_2
- `requires_bmi_check_vs1` id=`784f92af-36e1-c6e6-ccdd-1f02d8ec4fb0` (7/1 codes)

### ICB_CF_CVD_NHSHC_BMI_64_2
- `requires_bmi_check_vs1` id=`f4c3ca7e-5793-e57d-aea4-a7fd6e2d3135` (7/1 codes)

### ICB_CF_CVD_NHSHC_BMI_65_2
- `requires_bmi_check_vs1` id=`26d31088-3fbf-7f1f-75ea-71df6ab66354` (7/1 codes)

### ICB_CF_CVD_NHSHC_BMI_66_2
- `requires_bmi_check_vs1` id=`8d033e5d-cc1a-1093-ec7a-a056a4795a58` (7/1 codes)

### ICB_CF_CVD_NHSHC_BP_61_2
- `requires_bp_check_vs1` id=`33b1b48f-4c97-2919-b216-7631c86bf7a1` (17/1 codes)

### ICB_CF_CVD_NHSHC_BP_62_2
- `requires_bp_check_vs1` id=`2a70be7d-b327-1392-e6e8-d92303c2d7cc` (17/1 codes)

### ICB_CF_CVD_NHSHC_BP_63_2
- `requires_bp_check_vs1` id=`1bf57d15-b0fa-1895-54e9-3661c7bbaa80` (17/1 codes)

### ICB_CF_CVD_NHSHC_BP_64_2
- `requires_bp_check_vs1` id=`212af5f4-8a31-2ff1-e2d7-18cfccb2f7f6` (17/1 codes)

### ICB_CF_CVD_NHSHC_BP_65_2
- `requires_bp_check_vs1` id=`6593e03c-355b-12bc-a536-f85d8ca5b7fd` (17/1 codes)

### ICB_CF_CVD_NHSHC_BP_66_2
- `requires_bp_check_vs1` id=`dad18879-12ab-4918-80dd-93e626d7770f` (17/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_61_2
- `requires_cholesterol_check_vs1` id=`283d62c5-381e-63f9-0363-0b90c2760d91` (2/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_62_2
- `requires_cholesterol_check_vs1` id=`81a98c9a-7222-f828-7cc2-2f6336b502ba` (2/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_63_2
- `requires_cholesterol_check_vs1` id=`cdc66ab5-2502-6d84-d5c3-e68eea91abd8` (2/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_64_2
- `requires_cholesterol_check_vs1` id=`d065b60d-fcdd-a22f-3151-cb520b8e3fd0` (2/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_65_2
- `requires_cholesterol_check_vs1` id=`c7238a01-9090-654b-28f9-bb974ea0e60d` (2/1 codes)

### ICB_CF_CVD_NHSHC_CHOL_66_2
- `requires_cholesterol_check_vs1` id=`9070ac82-26dd-6733-5e0e-dcb35d518094` (2/1 codes)

### ICB_CF_CVD_NHSHC_HBA1C_61_2
- `requires_hba1c_check_vs1` id=`8f532167-fd77-6763-1870-464931e53dfd` (3/3 codes)

### ICB_CF_CVD_NHSHC_HBA1C_62_2
- `requires_hba1c_check_vs1` id=`b3969377-48ae-adec-f1dd-0acbc46559be` (3/3 codes)

### ICB_CF_CVD_NHSHC_HBA1C_63_2
- `requires_hba1c_check_vs1` id=`446780cf-120c-9a16-08de-ef480e9f0327` (3/3 codes)

### ICB_CF_CVD_NHSHC_HBA1C_64_2
- `requires_hba1c_check_vs1` id=`9047efa7-caf2-4bd3-1b2b-e0efdeba0eaa` (3/3 codes)

### ICB_CF_CVD_NHSHC_HBA1C_65_2
- `requires_hba1c_check_vs1` id=`e75af6a5-8abf-aaba-dda2-8c433d58942d` (3/3 codes)

### ICB_CF_CVD_NHSHC_HBA1C_66_2
- `requires_hba1c_check_vs1` id=`4fda9729-94a4-603a-12c6-d3d7fb6dc569` (3/3 codes)

### ICB_CF_CVD_NHSHC_LA_61_2
- `requires_lifestyle_advice_vs1` id=`3422bda8-1738-32fa-6be3-4a3e852330ac` (93/7 codes)

### ICB_CF_CVD_NHSHC_LA_62_2
- `requires_lifestyle_advice_vs1` id=`1896e8ca-066c-69b1-5d43-022b639e4fc9` (93/7 codes)

### ICB_CF_CVD_NHSHC_LA_63_2
- `requires_lifestyle_advice_vs1` id=`8da02434-744a-8971-510f-851f7219200a` (93/7 codes)

### ICB_CF_CVD_NHSHC_LA_64_2
- `requires_lifestyle_advice_vs1` id=`b1a3752e-9f4f-19fc-b73b-350f48537a1c` (93/7 codes)

### ICB_CF_CVD_NHSHC_LA_65_2
- `requires_lifestyle_advice_vs1` id=`8dc20fed-d6e4-97c4-c257-c2c8147e1f5f` (93/7 codes)

### ICB_CF_CVD_NHSHC_LA_66_2
- `requires_lifestyle_advice_vs1` id=`f9f800af-7963-349b-2b85-f52e006a78c0` (93/7 codes)

### ICB_CF_CVD_NHSHC_SMOK_61_2
- `requires_smoking_status_to_be_recorded_vs1` id=`b4fa3b85-c8bd-39e9-41df-3930bcfa0a58` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`fb82a7f9-e184-20bf-31f8-9c5753403251` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`f7c1816a-440e-ea25-2494-75882c96e2de` (0/1 codes)

### ICB_CF_CVD_NHSHC_SMOK_62_2
- `requires_smoking_status_to_be_recorded_vs1` id=`21f7c66d-9988-0091-9eff-64f43dee56c6` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`ce88fa13-f95a-0ebb-ff2d-5701ca1763aa` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`f628fa8f-d16d-c6be-810f-5fafc6ca45b7` (0/1 codes)

### ICB_CF_CVD_NHSHC_SMOK_63_2
- `requires_smoking_status_to_be_recorded_vs1` id=`5cc9d09c-4bbf-4193-bd64-fa74b1bc6f1c` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`ae65794b-368f-866d-622c-00f220c45076` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`e736f106-7ac3-536e-9fbb-5f4a04c972a7` (0/1 codes)

### ICB_CF_CVD_NHSHC_SMOK_64_2
- `requires_smoking_status_to_be_recorded_vs1` id=`65433e07-69b9-797d-58a5-bb491ccc7e9a` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`43ebb57f-a9aa-800f-4b69-f1013136b2be` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`e4756519-3fc1-c53e-5c3a-b07a14372641` (0/1 codes)

### ICB_CF_CVD_NHSHC_SMOK_65_2
- `requires_smoking_status_to_be_recorded_vs1` id=`068cab3d-e367-7f4a-5ab8-e5710e9cdfef` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`a7e5c373-dab3-3e20-89e2-7198dd1e007b` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`52945b0e-d1d3-b701-bb39-06bb7649c3e7` (0/1 codes)

### ICB_CF_CVD_NHSHC_SMOK_66_2
- `requires_smoking_status_to_be_recorded_vs1` id=`6c673bf9-2f78-6809-30a2-70aa3b5387df` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`f990e7b1-b55b-bee8-2529-adb1f5dfd6e4` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`4431c95d-0bb7-5e2a-a58f-b2c3fc678446` (0/1 codes)

### ICB_CF_CYPAST_61
- `asthma_casefinding_eligible_patients_vs1` id=`0bda5482-083e-41ba-a5eb-da032dd51b16` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_CYPAST_61_BASE
- `asthma_casefinding_vs1` id=`e49a6f96-d7a8-ad43-7448-2166a2858204` (1/2 codes)
- `asthma_casefinding_vs2` id=`be433490-3609-4849-20b6-045ddfbdc032` (126/226 codes)

### ICB_CF_CYPAST_61_woEX
- `asthma_casefinding_eligible_patients_vs1` id=`54940f0f-b140-2393-9d8d-0104b3103367` (469/469 codes) **[COLLISION->use id]**
- `asthma_casefinding_eligible_patients_vs2` id=`bca85ec2-6e34-97dd-4890-869a63673ca8` (0/8 codes)
- `asthma_casefinding_eligible_patients_vs3` id=`f1fdb838-2556-257c-fd9d-c621ad7ea0f3` (330/1 codes)
- `asthma_casefinding_eligible_patients_vs4` id=`3ad6ed82-b887-ed90-a8e7-254979cf33c1` (18/2 codes)
- `asthma_casefinding_eligible_patients_vs5` id=`48b40183-3fce-b10b-dcbe-3ef134f2426f` (105/2 codes)
- `asthma_casefinding_eligible_patients_vs6` id=`fbf51055-a383-65db-3bfe-7d5b3f508d1c` (103/1 codes)
- `asthma_casefinding_eligible_patients_vs7` id=`c61ece91-03d9-3fc8-3ea9-39a98be4ad5a` (1/1 codes)
- `asthma_casefinding_eligible_patients_vs8` id=`52527265-508f-4c80-7d04-ef7605e8b3e0` (1/1 codes)

### ICB_CF_DM_61
- `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` id=`877b18f5-9756-23a8-7a41-1900d12e70fa` (0/1 codes) **[COLLISION->use id]**

### ICB_CF_DM_61_woEX
- `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` id=`b7ce1049-b4c0-1477-ec22-7696854f03ed` (7/7 codes) **[COLLISION->use id]**
- `latest_hba1c_48_and_no_diagnosis_of_dm_vs2` id=`68968843-a4fb-1a67-29d0-bfe7f33b241e` (4/4 codes)
- `latest_hba1c_48_and_no_diagnosis_of_dm_vs3` id=`919fb132-083e-bba0-d8b6-d21e9846dcae` (3/3 codes)

### ICB_CF_DM_62
- `gestational_dm_and_no_hba1c_in_the_last_year_vs1` id=`5d6cf960-e4cd-9015-fd93-5105d7bf219a` (0/1 codes) **[COLLISION->use id]**

### ICB_CF_DM_62_BASE
- `patients_on_gestational_dm_vs1` id=`8827e417-8374-c4e2-a76b-d1b6f2c1ee5d` (10/5 codes)

### ICB_CF_DM_62_woEX
- `gestational_dm_and_no_hba1c_in_the_last_year_vs1` id=`56fd73ba-981e-cab3-0404-8c26b02afb36` (3/3 codes) **[COLLISION->use id]**

### ICB_CF_DM_63
- `latest_hba1c_46_47_and_no_hba1c_in_the_last_year_vs1` id=`776f1587-24f9-1d71-a7ff-6ff4e2a1c27d` (0/1 codes) **[COLLISION->use id]**

### ICB_CF_DM_63_woEX
- `latest_hba1c_46_47_and_no_hba1c_in_the_last_year_vs1` id=`160ec384-bb4f-915b-9a8e-66d118eefe0c` (3/3 codes) **[COLLISION->use id]**

### ICB_CF_DM_64
- `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` id=`4ed7513e-8c5c-68c8-3850-f4bff1387f79` (0/1 codes) **[COLLISION->use id]**

### ICB_CF_DM_64_BASE
- `obesity_with_latest_bmi_35_325_bame_population_vs1` id=`631c11e6-1a77-2cd0-0dd3-3ef61868842c` (7/1 codes)
- `obesity_with_latest_bmi_35_325_bame_population_vs2` id=`55c83e52-63f7-477c-82c8-9b172815352c` (159/7 codes)
- `obesity_with_latest_bmi_35_325_bame_population_vs3` id=`f0c4af8c-f818-da47-ba29-600c2f13e09f` (76/17 codes)

### ICB_CF_DM_64_woEX
- `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` id=`5bf8f69d-5b7e-b917-f3bf-4dcda11c8b6f` (3/3 codes) **[COLLISION->use id]**

### ICB_CF_DM_65
- `with_latest_bmi_30275bame_populationand_no_hba1c_in_l2y_vs1` id=`49bd264d-50de-3a1f-09e3-c06db976f89b` (0/1 codes)

### ICB_CF_DM_65_BASE
- `obesity_with_latest_bmi_30_35_275_325_bame_population_vs1` id=`75b5fe04-32b9-cb6b-1d1d-79b8183e75c4` (7/1 codes)
- `obesity_with_latest_bmi_30_35_275_325_bame_population_vs2` id=`2e9a5dbb-a1f2-19ff-d657-bf70de2d2c43` (159/7 codes)
- `obesity_with_latest_bmi_30_35_275_325_bame_population_vs3` id=`396f1048-e18e-908e-b0d3-958d23d46607` (76/17 codes)

### ICB_CF_DM_65_woEX
- `with_latest_bmi_30275bame_pop_no_hba1c_in_l2y_vs1` id=`dd384e6e-92e2-04b5-f1f9-17c119852411` (3/3 codes)

### ICB_CF_DM_66
- `latest_hba1c_42_45_and_no_hba1c_in_the_last_year_vs1` id=`343c45e1-2edb-f252-16d1-d6e1955659ea` (0/1 codes) **[COLLISION->use id]**

### ICB_CF_DM_66_woEX
- `latest_hba1c_42_45_and_no_hba1c_in_the_last_year_vs1` id=`a4dfc336-6ec7-4cec-21fa-c7d3e7842f2d` (3/3 codes) **[COLLISION->use id]**

### ICB_CF_DM_NDDP_61_1D
- `ndpp_eligible_population_vs1` id=`ff6e88cb-cd18-29d5-1a89-6acb021dfbd4` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`ebe15458-bdac-681e-5b16-c4c4ce126163` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`6d71556d-aee3-c532-01ab-94101a246f34` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`17ed3669-0e03-5fb2-23ac-bc95e3d1d94e` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`11a20655-85b1-a53e-ada1-c6d0227c8d16` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`e3123be3-25dd-7431-2297-6c98d4ee4216` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`5f62733f-b977-9f7c-b796-019aca7fa19c` (1/2 codes)

### ICB_CF_DM_NDDP_61_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`c504c744-5caa-0c42-ebfd-a319b0fe973c` (3/3 codes)

### ICB_CF_DM_NDDP_62_1D
- `ndpp_eligible_population_vs1` id=`1849f685-bc33-5589-6e55-b24c2ad51bd1` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`e3fc3da8-de3a-488c-a9ac-19736dc2a723` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`03f4dcd5-54f8-f275-6837-e4507797680f` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`dc8c85a6-95f0-d711-6fce-ab0cbffbb32f` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`1c0a94e2-c4fd-32f8-b830-6d18080dec13` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`007bc9a9-7612-1b56-b075-a87134a41fcb` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`b5a5fa97-0ff8-95e2-6014-105766c2de36` (1/2 codes)

### ICB_CF_DM_NDDP_62_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`dfd2a02e-9f67-0fe4-afbb-c7b5b8cd3065` (3/3 codes)

### ICB_CF_DM_NDDP_63_1D
- `ndpp_eligible_population_vs1` id=`ba5fb91e-4679-b020-4811-77ee9a21b3db` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`ca8038aa-9d90-7376-9d7f-72f17f478d0b` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`66ee8e0f-a712-eab3-3f19-63dd082ff925` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`9fd0a78e-d212-6d3f-c8d1-504af5777b5f` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`b5b1e852-aed3-5202-bc9c-28bb957b099b` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`f2305bcb-8160-4ae5-4f0e-08f22c6dbc16` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`51430f57-d766-e5f9-158d-2815f68d1671` (1/2 codes)

### ICB_CF_DM_NDDP_63_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`4dd235ff-df9a-2456-de72-6d1b404f5a3e` (3/3 codes)

### ICB_CF_DM_NDDP_64_1D
- `ndpp_eligible_population_vs1` id=`f6a9b92a-c43f-ae1d-cc77-384711c3b621` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`545071f5-d7f5-c4e5-f152-69e013b78dc5` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`fc6385ca-05fb-b371-4389-9b5f3ec00158` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`25ecc7ec-72c6-b0dc-5cf5-434de2cb42a1` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`498e7e55-f3b3-be90-0692-430816ed70c5` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`0956471a-8cbb-9ee7-dd1f-4ce5d4933574` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`c465e3b9-bf39-c300-919b-6cc14c9237d9` (1/2 codes)

### ICB_CF_DM_NDDP_64_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`faf0c40f-afd6-24ff-57c2-228c5f7f497b` (3/3 codes)

### ICB_CF_DM_NDDP_65_1D
- `ndpp_eligible_population_vs1` id=`8d77d3ce-09be-3bd0-ddd5-b8ebbd785f64` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`9a93aec2-c4c3-bfb4-facf-d71142d3706c` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`813aed41-b9da-b1c1-7b92-f3f6385be778` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`c3d6fdfe-6c73-dd2e-7a5e-61ca99ab917c` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`350351f1-5ab6-4bfb-959f-ab52b13fd52c` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`c98f03bf-6035-8183-3042-9669bbba1374` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`84d3ce61-6c96-28f9-bbaa-5de05694b4cd` (1/2 codes)

### ICB_CF_DM_NDDP_65_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`12930c73-b1e5-180b-29a8-8424f936435f` (3/3 codes)

### ICB_CF_DM_NDDP_66_1D
- `ndpp_eligible_population_vs1` id=`af2dd342-2268-41cb-c932-367520579637` (40/82 codes)
- `ndpp_eligible_population_vs2` id=`3c1ca940-1dc1-3bef-bbc5-ed762b5bc3aa` (0/1 codes)
- `ndpp_eligible_population_vs3` id=`ac91baf4-6951-1564-27b1-aeb78cae42e0` (3/1 codes)
- `ndpp_eligible_population_vs4` id=`c9e886d6-0d40-aa54-30e2-31bdd986e50b` (5/3 codes)
- `ndpp_eligible_population_vs5` id=`bfb549b2-cb30-3870-28be-d28568f266ec` (7/4 codes)
- `ndpp_eligible_population_vs6` id=`e48bfcff-3672-d0b0-cb8b-d561636d32f2` (3/3 codes)
- `ndpp_eligible_population_vs7` id=`b5ca2e39-8034-3794-443c-35c8f0f52dcd` (1/2 codes)

### ICB_CF_DM_NDDP_66_1N
- `eligible_for_ndpp_health_check_referral_vs1` id=`639011bc-5e22-d2ae-8759-82d8a2e3bab4` (3/3 codes)

### ICB_CF_DM_NHS_61_1N
- `requires_nhs_health_check_vs1` id=`a02e8bfd-ead7-3079-1a48-b4003f87a12c` (1/1 codes)

### ICB_CF_DM_NHS_62_1N
- `requires_nhs_health_check_vs1` id=`598f0018-243f-e5ab-a8fe-0b36771c88f6` (1/1 codes)

### ICB_CF_DM_NHS_63_1N
- `requires_nhs_health_check_vs1` id=`c7f25375-f274-66f1-c105-ddffdaa4a920` (1/1 codes)

### ICB_CF_DM_NHS_64_1N
- `requires_nhs_health_check_vs1` id=`7d71852a-f198-bc2f-778d-195628841848` (1/1 codes)

### ICB_CF_DM_NHS_65_1N
- `requires_nhs_health_check_vs1` id=`5184ce4a-8a8e-1150-65d0-7d84c47c32af` (1/1 codes)

### ICB_CF_DM_NHS_66_1N
- `requires_nhs_health_check_vs1` id=`c6cdcb18-18f9-79da-3af3-ebdeb1eb0cd2` (1/1 codes)

### ICB_CF_DM_NHS_BMI_61_2
- `requires_bmi_check_vs1` id=`1736d62c-8c47-becc-b691-5f744c11d74f` (7/1 codes)

### ICB_CF_DM_NHS_BMI_62_2
- `requires_bmi_check_vs1` id=`014a09e0-0148-be48-836f-edf08bfc0297` (7/1 codes)

### ICB_CF_DM_NHS_BMI_63_2
- `requires_bmi_check_vs1` id=`544a4321-bf48-f178-c70e-75a09fd82aa3` (7/1 codes)

### ICB_CF_DM_NHS_BMI_64_2
- `requires_bmi_check_vs1` id=`3ddc650d-3310-5029-40bf-5b2286643476` (7/1 codes)

### ICB_CF_DM_NHS_BMI_65_2
- `requires_bmi_check_vs1` id=`1e1c1643-40d5-e8c6-3f87-c876a4251bc3` (7/1 codes)

### ICB_CF_DM_NHS_BMI_66_2
- `requires_bmi_check_vs1` id=`3281b633-dbd1-c400-18c5-80399f1f6079` (7/1 codes)

### ICB_CF_DM_NHS_BP_61_2
- `requires_bp_check_vs1` id=`ea735e13-fe0f-1829-4fc2-1f2915f0881c` (17/1 codes)

### ICB_CF_DM_NHS_BP_62_2
- `requires_bp_check_vs1` id=`fc825dbc-167c-5402-1f5b-284d66823d60` (17/1 codes)

### ICB_CF_DM_NHS_BP_63_2
- `requires_bp_check_vs1` id=`4d567be2-e8b8-2abc-7967-cb2ff751aa2f` (17/1 codes)

### ICB_CF_DM_NHS_BP_64_2
- `requires_bp_check_vs1` id=`233c903c-fdb6-bc7a-f71f-f52d9fcc762a` (17/1 codes)

### ICB_CF_DM_NHS_BP_65_2
- `requires_bp_check_vs1` id=`d3b8bd78-7431-9396-3cdc-a0d972abda97` (17/1 codes)

### ICB_CF_DM_NHS_BP_66_2
- `requires_bp_check_vs1` id=`731fac31-6cc9-8083-c416-41e49d021791` (17/1 codes)

### ICB_CF_DM_NHS_CHOL_61_2
- `requires_cholesterol_check_vs1` id=`4d7446a0-9105-1569-338e-cc80d74e7ade` (2/1 codes)

### ICB_CF_DM_NHS_CHOL_62_2
- `requires_cholesterol_check_vs1` id=`a7efc7dd-6237-b4ce-d6c6-a47aded340a4` (2/1 codes)

### ICB_CF_DM_NHS_CHOL_63_2
- `requires_cholesterol_check_vs1` id=`f21f8857-60d5-c3e4-bf78-a67e5abb96bf` (2/1 codes)

### ICB_CF_DM_NHS_CHOL_64_2
- `requires_cholesterol_check_vs1` id=`eaa5ca72-ae8c-ad05-b333-3a0092089375` (2/1 codes)

### ICB_CF_DM_NHS_CHOL_65_2
- `requires_cholesterol_check_vs1` id=`bfab2a45-0e92-c737-8df0-4f961ef0632d` (2/1 codes)

### ICB_CF_DM_NHS_CHOL_66_2
- `requires_cholesterol_check_vs1` id=`a537b5c3-e749-b421-c990-d0e73ed881ce` (2/1 codes)

### ICB_CF_DM_NHS_HBA1C_61_2
- `requires_hba1c_check_vs1` id=`2699eaf6-209e-a7c3-b026-e9565d8649f1` (3/3 codes)

### ICB_CF_DM_NHS_HBA1C_62_2
- `requires_hba1c_check_vs1` id=`bd876fbb-f18b-4164-c2bd-de1707603e20` (3/3 codes)

### ICB_CF_DM_NHS_HBA1C_63_2
- `requires_hba1c_check_vs1` id=`49a417dd-3711-2fc5-f7cc-e1c16e22231e` (3/3 codes)

### ICB_CF_DM_NHS_HBA1C_64_2
- `requires_hba1c_check_vs1` id=`23544834-ddc7-3cc5-75ac-185b96b19deb` (3/3 codes)

### ICB_CF_DM_NHS_HBA1C_65_2
- `requires_hba1c_check_vs1` id=`a6dc8248-2b83-51d7-5030-1dffc3df5db6` (3/3 codes)

### ICB_CF_DM_NHS_HBA1C_66_2
- `requires_hba1c_check_vs1` id=`f0c7d89c-05f8-cb11-c1bb-b8381a0dc64c` (3/3 codes)

### ICB_CF_DM_NHS_LA_61_2
- `requires_lifestyle_advice_vs1` id=`a36fc426-f495-c549-c20a-44273f7afcbd` (93/7 codes)

### ICB_CF_DM_NHS_LA_62_2
- `requires_lifestyle_advice_vs1` id=`ed41a745-3bb1-7d27-b1c6-887ec740e207` (93/7 codes)

### ICB_CF_DM_NHS_LA_63_2
- `requires_lifestyle_advice_vs1` id=`70d45f94-d52e-3303-b6c6-255b12947b17` (93/7 codes)

### ICB_CF_DM_NHS_LA_64_2
- `requires_lifestyle_advice_vs1` id=`a67f4a8e-6c32-9054-2572-301ebfffba36` (93/7 codes)

### ICB_CF_DM_NHS_LA_65_2
- `requires_lifestyle_advice_vs1` id=`15bba8fe-78f8-75a1-ff64-4be6f527dabf` (93/7 codes)

### ICB_CF_DM_NHS_LA_66_2
- `requires_lifestyle_advice_vs1` id=`59f672f7-7e6a-b315-a9a3-e2f583f21a53` (93/7 codes)

### ICB_CF_DM_NHS_SMOK_61_2
- `requires_smoking_status_to_be_recorded_vs1` id=`1b7e0a54-f0a1-60b2-b7b7-2a7459db183c` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`0e71a1be-5332-8c37-5ebf-6b5b80d6beec` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`fc2add33-25c5-dfe9-7af4-dad90dce09b6` (0/1 codes)

### ICB_CF_DM_NHS_SMOK_62_2
- `requires_smoking_status_to_be_recorded_vs1` id=`cbc4a021-0a32-e7f4-c8e0-8dba1a35342a` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`6b0c8735-b30a-9332-ac67-d813a42883df` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`ed6f756c-c98e-47b8-9765-ee94fe1cca44` (0/1 codes)

### ICB_CF_DM_NHS_SMOK_63_2
- `requires_smoking_status_to_be_recorded_vs1` id=`c1948c98-dbba-d85c-5604-ed3d6c59c016` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`9f38ce68-41bd-2b8c-cbc4-ef909d5bc79a` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`325b3b2f-f95d-b6ec-628a-39d23fb90dd3` (0/1 codes)

### ICB_CF_DM_NHS_SMOK_64_2
- `requires_smoking_status_to_be_recorded_vs1` id=`9148a23f-2817-540e-ee63-d55c91b1a16c` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`242a3a38-a130-1901-9138-1c9558979dd9` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`c343cdfe-d28b-d329-6532-1c5063f825c5` (0/1 codes)

### ICB_CF_DM_NHS_SMOK_65_2
- `requires_smoking_status_to_be_recorded_vs1` id=`28e6f5dc-07b3-2f60-c143-82a46ffe63f7` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`f22ee44c-7f2f-2419-f110-b49aab3a5051` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`abd66f6a-082a-f1c2-267f-9f9c24689124` (0/1 codes)

### ICB_CF_DM_NHS_SMOK_66_2
- `requires_smoking_status_to_be_recorded_vs1` id=`1a9ac0da-5d13-cb11-0fef-11b10bd98bd5` (71/1 codes)
- `requires_smoking_status_to_be_recorded_vs2` id=`b469d21b-f893-5574-1728-80c243835faa` (2/1 codes)
- `requires_smoking_status_to_be_recorded_vs3` id=`2b9c594f-911e-52d3-c79a-6aeb67de08ad` (0/1 codes)

### ICB_CF_HF_61
- `hf_case_finding_eligible_patients_vs1` id=`9228a15e-e6cb-1dd2-6074-0c306c800c1b` (3/1 codes) **[COLLISION->use id]**

### ICB_CF_HF_61_BASE
- `eligible_for_hf_casefinding_vs1` id=`79ac6b27-da40-2a5c-116b-5de9d566e8c4` (42/7 codes)

### ICB_CF_HF_61_woEX
- `hf_case_finding_eligible_patients_vs1` id=`299f9e3b-bbab-c65e-c19e-9c531f91755a` (268/2 codes) **[COLLISION->use id]**
- `hf_case_finding_eligible_patients_vs2` id=`74eff833-06f4-a569-1b81-e72771737c77` (0/1 codes)
- `hf_case_finding_eligible_patients_vs3` id=`cf0f63ec-4ae0-7609-924a-92151a889b06` (126/2 codes)
- `hf_case_finding_eligible_patients_vs4` id=`20e1de18-362e-0e7d-acc2-f81a85d0b588` (202/4 codes)
- `hf_case_finding_eligible_patients_vs5` id=`61aa4ba5-cf4c-0827-9e4e-215b26de2be5` (7/3 codes)
- `hf_case_finding_eligible_patients_vs6` id=`aa5edbb4-da44-93e4-4ba1-2592ef9ad6c8` (10/7 codes)
- `hf_case_finding_eligible_patients_vs7` id=`da5cc6e9-85b0-c782-8c64-196be7d827f3` (3/1 codes)
- `hf_case_finding_eligible_patients_vs8` id=`b78f78bc-359c-5af3-e601-6906faeeca05` (193/18 codes)
- `hf_case_finding_eligible_patients_vs9` id=`e845a148-580e-15c2-d5d8-aee2d4be4766` (98/12 codes)
- `hf_case_finding_eligible_patients_vs10` id=`381f1b99-365b-eca0-2699-b854defd4329` (104/1 codes)
- `hf_case_finding_eligible_patients_vs11` id=`25bc8478-3999-0b84-68fb-3537a08cf9e9` (252/1 codes)
- `hf_case_finding_eligible_patients_vs12` id=`3e53fbd0-138d-f9e1-0b4c-185a063d7d17` (0/1 codes)
- `hf_case_finding_eligible_patients_vs13` id=`4674a3bf-df91-c098-51fc-f3c107eef488` (243/6 codes)

### ICB_CF_HTN_61
- `uclp_pg1_highest_risk_vs1` id=`166bb252-61af-1b31-8ebc-e651bbbd4f29` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_HTN_61_Base
- `uclp_priority_groups_base_vs1` id=`575e18a2-4be9-6188-9a72-e01df66ea55e` (2/2 codes)
- `uclp_priority_groups_base_vs2` id=`43172ab8-cb56-1f5c-e254-a840686326d3` (1/2 codes)
- `uclp_priority_groups_base_vs3` id=`45f38fcf-ca32-7e9d-a2a8-cf0f6d46cf5e` (97/181 codes)

### ICB_CF_HTN_61_woEX
- `uclp_pg1_highest_risk_vs1` id=`9f24e551-90ff-610d-61e7-ca822a831953` (16/22 codes) **[COLLISION->use id]**
- `uclp_pg1_highest_risk_vs2` id=`3b0120d1-f35d-1f81-0c0b-8d50f7d3be61` (8/15 codes)
- `uclp_pg1_highest_risk_vs3` id=`bb483a72-a3a9-a400-670d-3151fdc5dd8a` (16/22 codes)
- `uclp_pg1_highest_risk_vs4` id=`b9548f77-afe6-9cbd-45cd-fcd5426cbfa3` (8/15 codes)
- `uclp_pg1_highest_risk_vs5` id=`00fbdb88-3195-8d32-664f-558a94b6aa86` (8/7 codes)
- `uclp_pg1_highest_risk_vs6` id=`930c8ecf-7948-2be8-1b1f-9d5bbf1531cd` (8/7 codes)

### ICB_CF_HTN_62
- `uclp_priority_group_2a_vs1` id=`80f7370c-2495-0ff5-2c56-92fe5fd37f42` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_HTN_62_woEX
- `uclp_priority_group_2a_vs1` id=`dfa19d4f-2d74-deab-3a99-ab0ea818a3c5` (16/22 codes) **[COLLISION->use id]**
- `uclp_priority_group_2a_vs2` id=`0dab8359-09f5-aed2-9886-9eaea08b2e8c` (8/15 codes)
- `uclp_priority_group_2a_vs3` id=`f09bba55-6814-cd08-769d-5cb8013593bd` (16/22 codes)
- `uclp_priority_group_2a_vs4` id=`a2eece0b-4862-f2b3-7501-6e0da6447225` (8/15 codes)
- `uclp_priority_group_2a_vs5` id=`5b451d78-d8a1-6fb1-87c5-6f7e3002be2b` (8/7 codes)
- `uclp_priority_group_2a_vs6` id=`ac01c398-947d-5b6a-0c52-d6ea945c0d95` (8/7 codes)

### ICB_CF_HTN_63
- `uclp_priority_group_2b_vs1` id=`1e093c88-90aa-0c4e-adf1-e0a8daef3b5d` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_HTN_63_woEX
- `uclp_priority_group_2b_vs1` id=`feda64d2-aba7-a4ce-0365-8e7f0237d461` (74/78 codes) **[COLLISION->use id]**
- `uclp_priority_group_2b_vs2` id=`b73bfab1-6597-4c36-c461-05b329405509` (16/22 codes)
- `uclp_priority_group_2b_vs3` id=`23bd5b53-a643-811e-70a0-7727d40c5d93` (8/15 codes)
- `uclp_priority_group_2b_vs4` id=`54581074-60b7-92c8-ea63-029009b7db4b` (16/22 codes)
- `uclp_priority_group_2b_vs5` id=`947a18c4-03e9-b7cd-7e35-e1fac95192ae` (8/15 codes)
- `uclp_priority_group_2b_vs6` id=`d355026f-1434-0d6b-bcb4-27f1efb5d9ff` (8/7 codes)
- `uclp_priority_group_2b_vs7` id=`2b416d9c-91e0-fe7b-2a08-ee697365e88c` (8/7 codes)
- `uclp_priority_group_2b_vs8` id=`d853d61c-053d-233f-d1db-75d27e48b32d` (224/444 codes)
- `uclp_priority_group_2b_vs9` id=`1ba8a0da-5d96-c915-ff30-470ac2820ae4` (129/271 codes)
- `uclp_priority_group_2b_vs10` id=`cf3318be-288a-a51c-19f1-d4538edfad92` (12/38 codes)
- `uclp_priority_group_2b_vs11` id=`7f41bf53-89eb-04c9-c3c7-bde372a6491d` (6/32 codes)
- `uclp_priority_group_2b_vs12` id=`63575c00-64f0-9e1e-6fb0-c6294f64d93f` (51/106 codes)
- `uclp_priority_group_2b_vs13` id=`e5d016f8-4dba-d60f-2df3-a59bb5393342` (1/1 codes)
- `uclp_priority_group_2b_vs14` id=`141e47d8-2385-032e-c660-6c5f8783febd` (95/527 codes)
- `uclp_priority_group_2b_vs15` id=`2f955bb8-9d26-2b20-a0bf-7b00e7f46870` (1/5 codes)

### ICB_CF_HTN_65
- `uclp_priority_group_3a_vs1` id=`590709e2-0364-85a8-2fe6-ebf8af31cddb` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_HTN_65_woEX
- `uclp_priority_group_3a_vs1` id=`4df8a74f-daed-fda0-5a8d-81d6f775ab83` (16/22 codes) **[COLLISION->use id]**
- `uclp_priority_group_3a_vs2` id=`32805a24-cac2-65a8-cd53-1027a7650892` (8/15 codes)
- `uclp_priority_group_3a_vs3` id=`de63b11f-45d4-d463-666e-a9476005325e` (16/22 codes)
- `uclp_priority_group_3a_vs4` id=`b8923cba-7ca9-d7c1-5665-7468d4ff7672` (8/15 codes)
- `uclp_priority_group_3a_vs5` id=`ff7414fd-60ad-e784-91e6-13ff4a4b95fc` (8/7 codes)
- `uclp_priority_group_3a_vs6` id=`1527d417-22fe-efb0-a7bf-048cb76e097f` (8/7 codes)
- `uclp_priority_group_3a_vs7` id=`260808c2-3a75-1e80-2027-675d42a27713` (224/444 codes)
- `uclp_priority_group_3a_vs8` id=`41c1efb2-9145-93f3-c4a4-d404f8655581` (129/271 codes)
- `uclp_priority_group_3a_vs9` id=`1f67a8c0-1906-53ab-86ea-296c6ca65077` (12/38 codes)
- `uclp_priority_group_3a_vs10` id=`7ad8b08d-fd60-0866-ac33-3c169baccd91` (6/32 codes)
- `uclp_priority_group_3a_vs11` id=`147c0d1a-b8a0-7144-720e-9ea195cc60df` (51/106 codes)
- `uclp_priority_group_3a_vs12` id=`be233dce-6a9b-bb7f-c35c-37b0d59ba134` (1/1 codes)
- `uclp_priority_group_3a_vs13` id=`1ad8721e-0ae0-3e52-6f81-c7878d0500f2` (95/527 codes)
- `uclp_priority_group_3a_vs14` id=`aac129c1-d5fc-70d8-40fc-6ad3a532c9f9` (74/78 codes)

### ICB_CF_HTN_66
- `uclp_priority_group_3b_vs1` id=`a1a449b9-c2c3-792f-b08d-0142c3de3744` (1/1 codes) **[COLLISION->use id]**

### ICB_CF_HTN_66_woEX
- `uclp_priority_group_3b_vs1` id=`8c607c29-9273-b754-5056-727bbbc8d5fa` (16/22 codes) **[COLLISION->use id]**
- `uclp_priority_group_3b_vs2` id=`3da1afde-d14e-00f4-68d1-6f70aeb1ed26` (8/15 codes)
- `uclp_priority_group_3b_vs3` id=`f7c983f5-85b4-acb1-7545-16bca0cad009` (16/22 codes)
- `uclp_priority_group_3b_vs4` id=`9fc2a80c-2b4c-2414-0c1c-1d800ce06402` (8/15 codes)
- `uclp_priority_group_3b_vs5` id=`030a2e1d-75f9-4156-c3ed-0f7e536e500a` (8/7 codes)
- `uclp_priority_group_3b_vs6` id=`7381f4a0-ac61-a6fc-b9c3-e44891ef2bf5` (8/7 codes)

### ICB_CF_HTN_NHS_61_1N
- `requires_nhs_health_check_vs1` id=`e4c83b18-9350-f567-da14-8f2a1b96dd69` (1/1 codes)

### ICB_CF_HTN_NHS_61_2D
- `requires_bp_check_vs1` id=`896715fc-e3b6-95f0-1ab9-09998d1c6356` (17/1 codes)

### ICB_CF_HTN_NHS_62_1N
- `requires_nhs_health_check_vs1` id=`9e40eaae-993b-2f99-ab61-96febe18e1aa` (1/1 codes)

### ICB_CF_HTN_NHS_62_2N
- `requires_bp_check_vs1` id=`832dff0d-ed17-791d-7307-78562d2313b8` (17/1 codes)

### ICB_CF_HTN_NHS_63_1N
- `requires_nhs_health_check_vs1` id=`5fcd50ca-5175-5fd3-93f7-6f740dd2e342` (1/1 codes)

### ICB_CF_HTN_NHS_63_2N
- `requires_bp_check_vs1` id=`2ee150ed-365f-f44e-b466-46125b3ed982` (17/1 codes)

### ICB_CF_HTN_NHS_65_1N
- `requires_nhs_health_check_vs1` id=`2564697e-e46b-b672-6dc9-11b7da80b7d4` (1/1 codes)

### ICB_CF_HTN_NHS_65_2N
- `requires_bp_check_vs1` id=`6ae31836-7ce8-51ab-7f79-2b9d47ba5538` (17/1 codes)

### ICB_CF_HTN_NHS_66_1N
- `requires_nhs_health_check_vs1` id=`66359179-d9df-3a0b-3c01-f2f10a648558` (1/1 codes)

### ICB_CF_HTN_NHS_66_2N
- `requires_bp_check_vs1` id=`573ce6c0-31c6-da67-6879-a54401429094` (17/1 codes)

### ICS_METABOLIC_LTC
- `metabolic_conditions_vs1` id=`3b4d5851-8dbb-2c24-7899-5ac257a8e478` (63/1 codes)

### NHSHC_ELIGIBLE
- `eligible_for_nhs_health_check_vs1` id=`b658cdc3-458e-07d4-731e-26b106c1e6d2` (0/1 codes)