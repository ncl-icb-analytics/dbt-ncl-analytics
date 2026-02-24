"""Batch embedding pipeline: reads descriptions, embeds them, writes vectors to parquet."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from .models import BaseEmbedder, ModelConfig

logger = logging.getLogger(__name__)


def embed_descriptions(
    embedder: BaseEmbedder,
    input_path: Path,
    output_path: Path,
    text_column: str = "term",
    id_column: str = "description_id",
    chunk_size: int = 50_000,
) -> None:
    """Embed all descriptions from a parquet file and write vectors to output.

    Processes in chunks to manage memory. Each chunk is appended to the
    output parquet file.

    Args:
        embedder: Loaded embedding model.
        input_path: Path to input parquet with descriptions.
        output_path: Path to write output parquet with vectors.
        text_column: Column name containing text to embed.
        id_column: Column name containing unique identifier.
        chunk_size: Number of rows to process at a time.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    writer = None

    try:
        for batch in tqdm(
            parquet_file.iter_batches(batch_size=chunk_size, columns=[id_column, text_column]),
            total=(total_rows + chunk_size - 1) // chunk_size,
            desc=f"Embedding with {embedder.config.name}",
        ):
            df = batch.to_pandas()
            texts = df[text_column].fillna("").tolist()
            ids = df[id_column].tolist()

            vectors = embedder.encode(texts)

            # Build output table
            table = pa.table(
                {
                    id_column: ids,
                    "vector": [v.tolist() for v in vectors],
                }
            )

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)

            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()

    logger.info("Wrote %d vectors to %s", total_rows, output_path)


def embed_all_models(
    configs: list[ModelConfig],
    descriptions_path: Path,
    output_dir: Path,
    text_column: str = "term",
    id_column: str = "description_id",
) -> dict[str, Path]:
    """Run embedding for all configured models.

    Args:
        configs: List of model configurations.
        descriptions_path: Path to parquet with descriptions to embed.
        output_dir: Base directory for vector outputs.

    Returns:
        Dict mapping model name to output parquet path.
    """
    from .models import create_embedder

    results = {}

    for config in configs:
        logger.info("Starting embeddings for model: %s", config.name)

        output_path = output_dir / config.name.lower().replace(" ", "_") / "vectors.parquet"

        embedder = create_embedder(config)
        embedder.load()

        try:
            embed_descriptions(
                embedder=embedder,
                input_path=descriptions_path,
                output_path=output_path,
                text_column=text_column,
                id_column=id_column,
            )
            results[config.name] = output_path
        finally:
            embedder.unload()

    return results
