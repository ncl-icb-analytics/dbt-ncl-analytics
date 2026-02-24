"""Data preparation: export concept map and SNOMED descriptions for embedding."""

from __future__ import annotations

import logging
from pathlib import Path

import click

from .utils.snowflake import export_concept_map, export_snomed_descriptions

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data"


@click.command()
@click.option("--data-dir", type=click.Path(), default=str(DEFAULT_DATA_DIR), help="Output directory for data files")
@click.option("--concept-map/--no-concept-map", default=True, help="Export concept map")
@click.option("--snomed/--no-snomed", default=True, help="Export SNOMED descriptions")
def prepare_data(data_dir: str, concept_map: bool, snomed: bool) -> None:
    """Export source data from Snowflake for the embedding pipeline."""
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    if concept_map:
        cm_path = data_path / "concept_map.parquet"
        count = export_concept_map(cm_path)
        click.echo(f"Exported {count:,} concept map rows to {cm_path}")

    if snomed:
        snomed_path = data_path / "snomed_descriptions.parquet"
        count = export_snomed_descriptions(snomed_path)
        click.echo(f"Exported {count:,} SNOMED descriptions to {snomed_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    prepare_data()
