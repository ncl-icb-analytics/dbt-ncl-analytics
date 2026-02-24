"""FAISS index building and querying for nearest-neighbour search over SNOMED vectors."""

from __future__ import annotations

import logging
from pathlib import Path

import faiss
import numpy as np
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class VectorIndex:
    """Wraps a FAISS index with ID mapping for SNOMED description lookup."""

    def __init__(self, dimensions: int, index_type: str = "IVFFlat", nlist: int = 4096):
        self.dimensions = dimensions
        self.index_type = index_type
        self.nlist = nlist
        self._index: faiss.Index | None = None
        self._id_map: list[str] = []  # Maps internal index position → description_id

    def build(self, vectors_path: Path, id_column: str = "description_id") -> None:
        """Build the FAISS index from a vectors parquet file.

        Args:
            vectors_path: Parquet file with id_column and 'vector' columns.
            id_column: Name of the ID column.
        """
        logger.info("Loading vectors from %s", vectors_path)
        table = pq.read_table(vectors_path, columns=[id_column, "vector"])

        ids = table.column(id_column).to_pylist()
        vectors_list = table.column("vector").to_pylist()
        vectors = np.array(vectors_list, dtype=np.float32)

        n, d = vectors.shape
        assert d == self.dimensions, f"Expected {self.dimensions}d vectors, got {d}d"

        logger.info("Building %s index for %d vectors of dimension %d", self.index_type, n, d)

        if self.index_type == "Flat":
            self._index = faiss.IndexFlatIP(d)  # Inner product (vectors are normalised)
        elif self.index_type == "IVFFlat":
            quantizer = faiss.IndexFlatIP(d)
            self._index = faiss.IndexIVFFlat(quantizer, d, min(self.nlist, n // 10))
            self._index.train(vectors)
        else:
            raise ValueError(f"Unknown index type: {self.index_type}")

        self._index.add(vectors)
        self._id_map = ids

        logger.info("Index built with %d vectors", self._index.ntotal)

    def search(self, query_vectors: np.ndarray, k: int = 20, nprobe: int = 32) -> list[list[dict]]:
        """Search the index for nearest neighbours.

        Args:
            query_vectors: Array of shape (n_queries, dimensions).
            k: Number of nearest neighbours to return.
            nprobe: Number of clusters to search (IVF only).

        Returns:
            List of lists of dicts with 'description_id' and 'score'.
        """
        if self._index is None:
            raise RuntimeError("Index not built. Call build() first.")

        if hasattr(self._index, "nprobe"):
            self._index.nprobe = nprobe

        scores, indices = self._index.search(query_vectors, k)

        results = []
        for i in range(len(query_vectors)):
            neighbours = []
            for j in range(k):
                idx = indices[i][j]
                if idx == -1:
                    break
                neighbours.append(
                    {
                        "description_id": self._id_map[idx],
                        "score": float(scores[i][j]),
                    }
                )
            results.append(neighbours)

        return results

    def save(self, path: Path) -> None:
        """Save the index and ID map to disk."""
        path.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self._index, str(path / "index.faiss"))
        np.save(str(path / "id_map.npy"), np.array(self._id_map))
        logger.info("Index saved to %s", path)

    def load(self, path: Path) -> None:
        """Load a previously saved index from disk."""
        self._index = faiss.read_index(str(path / "index.faiss"))
        self._id_map = np.load(str(path / "id_map.npy"), allow_pickle=True).tolist()
        logger.info("Index loaded from %s (%d vectors)", path, self._index.ntotal)
