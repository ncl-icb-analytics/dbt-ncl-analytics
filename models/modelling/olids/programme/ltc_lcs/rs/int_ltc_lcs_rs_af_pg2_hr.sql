

-- LTC LCS DBT extract
--
--  AF - HR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

--Rule 1: On oral anticoagulants OR DOACs (Dabigatran, Rivaroxaban, Apixaban, Edoxaban, Acenocoumarol, Phenindione) in last 6 months OR Anticoagulant prescribed by third party code
--Rule 2: On antiplatelet therapy (Aspirin, Clopidogrel, Prasugrel, Ticagrelor, Dipyridamole) OR aspirin prophylaxis codes in last 6 months
--Rule 3: HAS-BLED score ≥3 OR ORBIT-AF bleeding risk score ≥4
--Rule 4: On DOACs in last 6 months
--Rule 5: Cockcroft-Gault creatinine clearance <40 OR body weight <50kg OR Rockwood Frailty level 7-9 (severely frail to terminally ill) OR Frailty score ≥7

with AF_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER
    )  

select distinct DR.person_id from AF_reg DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr_r1 R1
on DR.person_id = R1.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr_r2 R2
on DR.person_id = R2.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr_r3 R3
on DR.person_id = R3.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr_r4 R4
on DR.person_id = R4.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr_r5 R5
on DR.person_id = R5.person_id

--Rule 1: On oral anticoagulants OR DOACs (Dabigatran, Rivaroxaban, Apixaban, Edoxaban, Acenocoumarol, Phenindione) in last 6 months OR Anticoagulant prescribed by third party code
where 
R1.person_id is not null
and
    (
--Rule 2: On antiplatelet therapy (Aspirin, Clopidogrel, Prasugrel, Ticagrelor, Dipyridamole) OR aspirin prophylaxis codes in last 6 months
    R2.person_id is not null

--Rule 3: HAS-BLED score ≥3 OR ORBIT-AF bleeding risk score ≥4
    or
    R3.person_id is not null

--Rule 4: On DOACs in last 6 months
    or
        (R4.person_id is not null
        and
        R5.person_id is not null)
    )

--Rule 5: EXCLUDE IF Cockcroft-Gault creatinine clearance <40 OR body weight <50kg OR Rockwood Frailty level 7-9 (severely frail to terminally ill) OR Frailty score ≥7
