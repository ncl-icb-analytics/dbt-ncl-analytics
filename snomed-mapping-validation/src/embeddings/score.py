"""Similarity scoring and flagging logic for concept map validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .index import VectorIndex

logger = logging.getLogger(__name__)


@dataclass
class ScoringResult:
    """Result of scoring a single concept map row against one model."""

    source_code_id: str
    target_code_id: str
    model_name: str
    pair_cosine_similarity: float
    target_rank_in_top_k: int | None  # None if not found in top-K
    best_alternative_description_id: str | None
    best_alternative_concept_id: str | None
    best_alternative_similarity: float | None
    best_alternative_term: str | None


def load_vector_lookup(vectors_path: Path, id_column: str = "description_id") -> dict[str, np.ndarray]:
    """Load vectors into a dict keyed by description_id for fast lookup."""
    table = pq.read_table(vectors_path, columns=[id_column, "vector"])
    ids = table.column(id_column).to_pylist()
    vectors = table.column("vector").to_pylist()

    return {did: np.array(vec, dtype=np.float32) for did, vec in zip(ids, vectors)}


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


def score_concept_map(
    concept_map: pd.DataFrame,
    vector_lookup: dict[str, np.ndarray],
    index: VectorIndex,
    description_lookup: pd.DataFrame,
    model_name: str,
    top_k: int = 20,
) -> pd.DataFrame:
    """Score all concept map rows against a single embedding model.

    Args:
        concept_map: DataFrame with source_code_id, source_display,
                     target_code_id, target_display, etc.
        vector_lookup: Dict mapping description_id → vector.
        index: FAISS index over full SNOMED descriptions.
        description_lookup: DataFrame mapping description_id → concept_id, term.
        model_name: Name of the embedding model.
        top_k: Number of nearest neighbours to retrieve.

    Returns:
        DataFrame of scoring results.
    """
    results = []

    for _, row in concept_map.iterrows():
        source_vec = vector_lookup.get(str(row["source_code_id"]))
        target_vec = vector_lookup.get(str(row["target_code_id"]))

        if source_vec is None or target_vec is None:
            continue

        # Direct pair similarity
        pair_sim = cosine_similarity(source_vec, target_vec)

        # Search for nearest neighbours to source
        neighbours = index.search(source_vec.reshape(1, -1), k=top_k)[0]

        # Find where the current target ranks
        target_rank = None
        for rank, neighbour in enumerate(neighbours, 1):
            if neighbour["description_id"] == str(row["target_code_id"]):
                target_rank = rank
                break

        # Best alternative (not the current target)
        best_alt = None
        for neighbour in neighbours:
            if neighbour["description_id"] != str(row["target_code_id"]):
                best_alt = neighbour
                break

        best_alt_desc_id = best_alt["description_id"] if best_alt else None
        best_alt_score = best_alt["score"] if best_alt else None

        # Look up the alternative's concept_id and term
        best_alt_concept_id = None
        best_alt_term = None
        if best_alt_desc_id is not None:
            alt_row = description_lookup.loc[
                description_lookup["description_id"] == best_alt_desc_id
            ]
            if not alt_row.empty:
                best_alt_concept_id = str(alt_row.iloc[0]["concept_id"])
                best_alt_term = str(alt_row.iloc[0]["term"])

        results.append(
            {
                "source_code_id": row["source_code_id"],
                "target_code_id": row["target_code_id"],
                "model_name": model_name,
                "pair_cosine_similarity": pair_sim,
                "target_rank_in_top_k": target_rank,
                "best_alternative_description_id": best_alt_desc_id,
                "best_alternative_concept_id": best_alt_concept_id,
                "best_alternative_similarity": best_alt_score,
                "best_alternative_term": best_alt_term,
            }
        )

    return pd.DataFrame(results)


def flag_mappings(
    scores: pd.DataFrame,
    similarity_threshold: float = 0.70,
    top_k_threshold: int = 5,
    better_alt_margin: float = 0.10,
) -> pd.DataFrame:
    """Flag mappings that appear suspect based on scoring results.

    A mapping is flagged if:
    - pair_cosine_similarity < similarity_threshold
    - target_rank_in_top_k > top_k_threshold (or not found)
    - best_alternative_similarity > pair_cosine_similarity + margin

    Args:
        scores: Output of score_concept_map.
        similarity_threshold: Minimum acceptable similarity.
        top_k_threshold: Target must be within this rank.
        better_alt_margin: How much better an alternative must be.

    Returns:
        Subset of scores where at least one flag is raised, with flag columns.
    """
    df = scores.copy()

    df["flag_low_similarity"] = df["pair_cosine_similarity"] < similarity_threshold

    df["flag_low_rank"] = df["target_rank_in_top_k"].isna() | (
        df["target_rank_in_top_k"] > top_k_threshold
    )

    df["flag_better_alternative"] = (
        df["best_alternative_similarity"].notna()
        & (df["best_alternative_similarity"] > df["pair_cosine_similarity"] + better_alt_margin)
    )

    df["is_flagged"] = df["flag_low_similarity"] | df["flag_low_rank"] | df["flag_better_alternative"]

    return df[df["is_flagged"]].copy()


def aggregate_flags_across_models(
    all_scores: list[pd.DataFrame],
    min_models_agreeing: int = 2,
) -> pd.DataFrame:
    """Combine flags from multiple models. Only keep mappings flagged by N+ models.

    Args:
        all_scores: List of flagged DataFrames from each model.
        min_models_agreeing: Minimum number of models that must flag a mapping.

    Returns:
        DataFrame of mappings flagged by at least min_models_agreeing models.
    """
    combined = pd.concat(all_scores, ignore_index=True)

    # Count how many models flagged each mapping
    flag_counts = (
        combined.groupby(["source_code_id", "target_code_id"])
        .agg(
            models_flagging=("model_name", "nunique"),
            avg_similarity=("pair_cosine_similarity", "mean"),
            min_similarity=("pair_cosine_similarity", "min"),
        )
        .reset_index()
    )

    # Filter to those with enough model agreement
    multi_flagged = flag_counts[flag_counts["models_flagging"] >= min_models_agreeing]

    # Merge back the per-model detail
    result = combined.merge(
        multi_flagged[["source_code_id", "target_code_id"]],
        on=["source_code_id", "target_code_id"],
        how="inner",
    )

    logger.info(
        "Flagged %d unique mappings (from %d model-level flags, requiring %d+ models)",
        multi_flagged.shape[0],
        combined.shape[0],
        min_models_agreeing,
    )

    return result
