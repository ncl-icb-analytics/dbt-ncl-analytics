# LTC LCS R5 case finding

EMIS implementation-guide extraction for the R5 "1) Casefinding R2" searches.

- [Generated specification guides](specs/INDEX.md)

The markdown guides are extracted from `NCL LTC LCS R5 updated 27112025.xml` (sha256 `e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8`) via the EMIS XML agent API (`getImplementationGuide`). 177 reports across seven case-finding conditions:

| Condition | Folder | Reports |
| --- | --- | --- |
| af | [CF-AF] AF Case finding | 8 |
| ckd | [CF-CKD] CKD Case finding | 10 |
| cvd | [CF-CVD] High risk CVD | 60 |
| asthma_cyp | [CF-CYPAST] possible diagnosis of asthma (CYP) | 1 |
| diabetes | [CF-DM] High risk Diabetes | 72 |
| hf | [CF-HF] Heart Failure | 1 |
| hypertension | [CF-HTN] Raised BP (Possible HTN) - UCLP Criteria | 25 |

Each guide's "Start population" section inlines the upstream `_BASE` / `_woEX` supporting searches (the `zSupporting Searches > CF` folder), so the parent logic is captured without separate files.

The guides are a readable summary. For exact operators, thresholds and ranges, query the agent API (`agentInterpretation.decisionFlow[].criteriaDetails`) against the same XML.
