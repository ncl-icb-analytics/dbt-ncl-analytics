# LTC LCS R5 risk stratification

This directory keeps the R5 EMIS implementation-guide extraction and the canonical issue screenshots together:

- [Generated specification guides](specs/INDEX.md)
- [Canonical EMIS screenshots by condition](screenshots/README.md)

The markdown guides are extracted from `NCL LTC LCS R5 updated 27112025.xml`; the screenshots come from GitHub issues #393-#406 and supplement the generated guides where the live EMIS UI is the source of truth.

## Refreshing specs

Regenerate the extracted guides with:

```powershell
python scripts\extract_emis_ltc_lcs_specs.py
```

The script treats `specs/` as generated output: it removes existing markdown, refetches the XML-derived guides, and rewrites the condition-organised tree. Register-specific searches are written under `specs/conditions/<condition>/`; cross-condition building blocks are written under `specs/shared/<area>/`.
