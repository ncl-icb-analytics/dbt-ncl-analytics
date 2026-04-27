"""Generate seeds/uk_cost_indices.csv from HMT GDP deflator + ONS CPI / CPIH.

Sources (all authoritative and free):
- HMT GDP deflator series (financial year): March 2026 Quarterly National Accounts
  release. Outturn 1955-56 to 2024-25 from ONS series L8GG; forecasts 2025-26
  to 2030-31 from OBR Spring Statement March 2026 EFO.
  https://www.gov.uk/government/collections/gdp-deflators-at-market-prices-and-money-gdp
- ONS CPI all-items index (series D7BT, base 2015=100), monthly.
  https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/d7bt/mm23
- ONS CPIH all-items index (series L522, base 2015=100), monthly.
  https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/l522/mm23

Fiscal year convention: UK financial year runs 1 April YYYY to 31 March YYYY+1.
- GDP deflator is published directly in fiscal years.
- CPI / CPIH are published as monthly indices. Fiscal year values here are the
  unweighted mean of the 12 monthly indices from Apr YYYY to Mar YYYY+1.

Base years are kept as published (GDP deflator 2024-25=100, CPI/CPIH 2015=100).
Downstream users rebase by dividing by the target-year value.

is_forecast is TRUE only for GDP deflator rows 2025-26 onwards (OBR forecast).
CPI / CPIH values for a fiscal year are only populated when all 12 monthly
observations are available — partial years are NULL.

Re-run this script with the latest HMT XLSX and a fresh ONS fetch to refresh
the seed. See `fetch_sources.sh` for the one-liner.
"""

from __future__ import annotations

import json
from pathlib import Path

import openpyxl

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
SEED_PATH = REPO_ROOT / "seeds" / "uk_cost_indices.csv"

# Metadata for the seed header row
GDP_SOURCE = "HMT GDP Deflator March 2026 Quarterly National Accounts"
CPI_SOURCE = "ONS CPI all items index (D7BT), MM23 release 2026-03"
CPIH_SOURCE = "ONS CPIH all items index (L522), MM23 release 2026-03"


def parse_gdp_deflator() -> dict[int, dict]:
    """Return {fiscal_year_start: {'deflator': float, 'is_forecast': bool}}.

    fiscal_year_start is the calendar year the fiscal year begins (e.g. 2023
    for 2023-24). Forecast years have their index chain-computed from the
    2024-25 = 100 base using the OBR percent-change values.
    """
    wb = openpyxl.load_workbook(
        HERE / "gdp_deflators_march_2026.xlsx", data_only=True
    )
    sh = wb.active

    out: dict[int, dict] = {}
    # Data starts at row 8; stop at "Sources and footnotes:" line
    last_outturn_index: float | None = None

    for row in sh.iter_rows(min_row=8, values_only=True):
        fy_label = row[1]
        idx_val = row[2]
        pct_change = row[3]

        if not isinstance(fy_label, str) or "-" not in fy_label:
            continue
        if "Sources" in fy_label or "Footnote" in fy_label:
            break

        # Strip OBR footnote markers "(1), (2)" from forecast rows
        clean = fy_label.split(" ")[0]  # "2025-26"
        try:
            year_start = int(clean.split("-")[0])
        except ValueError:
            continue

        is_forecast = "(" in fy_label  # HMT flags forecast rows with footnotes

        if not is_forecast and isinstance(idx_val, (int, float)):
            out[year_start] = {
                "deflator": float(idx_val),
                "is_forecast": False,
            }
            last_outturn_index = float(idx_val)
        elif is_forecast and isinstance(pct_change, (int, float)):
            # Chain-compute from the previous year's index using the pct change.
            # Guard against the previous year being missing — fall back to the
            # most recent available year, or skip the row if no prior year
            # exists at all (avoids a confusing KeyError on partial data).
            if (year_start - 1) in out:
                prev = out[year_start - 1]["deflator"]
            else:
                earlier_years = [k for k in out.keys() if k < year_start]
                if not earlier_years:
                    print(
                        f"WARNING: skipping forecast year {year_start} — "
                        f"no prior year in series to chain from"
                    )
                    continue
                prev = out[max(earlier_years)]["deflator"]
            deflator = prev * (1 + pct_change / 100)
            out[year_start] = {
                "deflator": round(deflator, 4),
                "is_forecast": True,
            }

    return out


def parse_ons_monthly(json_path: Path) -> dict[str, float]:
    """Return {'YYYY-MM': value} from an ONS time series JSON dump."""
    data = json.loads(json_path.read_text(encoding="utf-8"))
    months: dict[str, float] = {}
    month_name_to_num = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12",
    }
    for m in data.get("months", []):
        year = m.get("year")
        month = month_name_to_num.get(m.get("month"))
        val = m.get("value")
        if year and month and val not in (None, "", "."):
            try:
                months[f"{year}-{month}"] = float(val)
            except ValueError:
                continue
    return months


def fiscal_year_mean(
    monthly: dict[str, float], fiscal_year_start: int
) -> float | None:
    """Mean of Apr YYYY through Mar YYYY+1. Returns None if any month missing."""
    keys = [f"{fiscal_year_start}-{m:02d}" for m in range(4, 13)] + [
        f"{fiscal_year_start + 1}-{m:02d}" for m in range(1, 4)
    ]
    vals = [monthly.get(k) for k in keys]
    if any(v is None for v in vals):
        return None
    return round(sum(vals) / len(vals), 4)  # type: ignore[arg-type]


def main() -> None:
    gdp = parse_gdp_deflator()
    cpi = parse_ons_monthly(HERE / "cpi_d7bt.csv")
    cpih = parse_ons_monthly(HERE / "cpih_l522.csv")

    # Range: start at 2000-01 (modern OLIDS appointments, plenty of history)
    # end at the last year HMT publishes a forecast for (2030-31)
    fiscal_years = sorted(y for y in gdp if y >= 2000)

    rows = [
        "fiscal_year_start,fiscal_year,gdp_deflator,cpi_index,cpih_index,is_forecast"
    ]
    for y in fiscal_years:
        fy_label = f"{y}-{str(y + 1)[-2:]}"
        g = gdp[y]
        cpi_val = fiscal_year_mean(cpi, y)
        cpih_val = fiscal_year_mean(cpih, y)
        rows.append(
            ",".join(
                [
                    str(y),
                    fy_label,
                    f"{g['deflator']:.4f}",
                    "" if cpi_val is None else f"{cpi_val:.4f}",
                    "" if cpih_val is None else f"{cpih_val:.4f}",
                    "true" if g["is_forecast"] else "false",
                ]
            )
        )

    SEED_PATH.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"Wrote {len(rows) - 1} rows to {SEED_PATH}")

    # Quick sanity print -- show years around the outturn/forecast boundary.
    # Iterate over the years we actually have so the print never KeyErrors
    # on a partial HMT release.
    print("\nBoundary years (outturn -> forecast):")
    print("  fy      | deflator | cpi      | cpih     | forecast?")
    boundary_years = sorted(y for y in gdp if 2020 <= y <= 2030)
    for y in boundary_years:
        g = gdp[y]
        cpi_val = fiscal_year_mean(cpi, y)
        cpih_val = fiscal_year_mean(cpih, y)
        print(
            f"  {y}-{str(y + 1)[-2:]} | "
            f"{g['deflator']:>8.4f} | "
            f"{'-' if cpi_val is None else f'{cpi_val:>8.4f}'} | "
            f"{'-' if cpih_val is None else f'{cpih_val:>8.4f}'} | "
            f"{'F' if g['is_forecast'] else '.'}"
        )


if __name__ == "__main__":
    main()
