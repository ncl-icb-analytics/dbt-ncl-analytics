"""CLI entry point for the SNOMED mapping validation pipeline."""

from __future__ import annotations

import logging

import click


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def main(verbose: bool) -> None:
    """SNOMED CT Concept Map Validation Pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


@main.command()
@click.option("--data-dir", type=click.Path(), default="data", help="Output directory")
def prepare(data_dir: str) -> None:
    """Export concept map and SNOMED descriptions from Snowflake."""
    from .data_prep import prepare_data

    ctx = click.Context(prepare_data)
    ctx.invoke(prepare_data, data_dir=data_dir)


@main.command()
@click.option("--data-dir", type=click.Path(), default="data", help="Data directory")
@click.option("--model", type=str, default=None, help="Run only a specific model (by config key)")
@click.option("--config", type=click.Path(), default="config/models.yaml", help="Model config file")
def embed(data_dir: str, model: str | None, config: str) -> None:
    """Run embedding models over descriptions."""
    from pathlib import Path

    import yaml

    from .embeddings.embed import embed_all_models
    from .embeddings.models import ModelConfig

    with open(config) as f:
        cfg = yaml.safe_load(f)

    configs = []
    for key, mcfg in cfg["models"].items():
        if model and key != model:
            continue
        configs.append(
            ModelConfig(
                name=mcfg["name"],
                model_id=mcfg["model_id"],
                model_type=mcfg["type"],
                dimensions=mcfg["dimensions"],
                batch_size=mcfg["batch_size"],
                similarity_threshold=mcfg.get("similarity_threshold", 0.70),
                top_k_threshold=mcfg.get("top_k_threshold", 5),
                precision=mcfg.get("precision", "fp16"),
                provider=mcfg.get("provider"),
                reduced_dimensions=mcfg.get("reduced_dimensions"),
                requests_per_minute=mcfg.get("requests_per_minute"),
            )
        )

    data_path = Path(data_dir)

    # Embed SNOMED descriptions
    snomed_path = data_path / "snomed_descriptions.parquet"
    if snomed_path.exists():
        click.echo("Embedding SNOMED descriptions...")
        embed_all_models(configs, snomed_path, data_path / "vectors" / "snomed")

    # Embed concept map descriptions (both source and target)
    cm_path = data_path / "concept_map.parquet"
    if cm_path.exists():
        click.echo("Embedding concept map descriptions...")
        embed_all_models(
            configs, cm_path, data_path / "vectors" / "concept_map",
            text_column="source_display", id_column="source_code_id",
        )


@main.command()
@click.option("--data-dir", type=click.Path(), default="data", help="Data directory")
def score(data_dir: str) -> None:
    """Compute similarity scores and flag suspect mappings."""
    click.echo("Scoring not yet implemented - coming in Phase 1")


@main.command()
@click.option("--data-dir", type=click.Path(), default="data", help="Data directory")
@click.option("--output", type=click.Path(), default="data/flagged_mappings.parquet")
def flag(data_dir: str, output: str) -> None:
    """Aggregate flags across models and output flagged mappings."""
    click.echo("Flagging not yet implemented - coming in Phase 1")


if __name__ == "__main__":
    main()
