"""Embedding model abstractions for local and API-based models."""

from __future__ import annotations

import abc
from dataclasses import dataclass

import numpy as np


@dataclass
class ModelConfig:
    """Configuration for an embedding model."""

    name: str
    model_id: str
    model_type: str  # "local" or "api"
    dimensions: int
    batch_size: int
    similarity_threshold: float
    top_k_threshold: int
    precision: str = "fp16"
    provider: str | None = None
    reduced_dimensions: int | None = None
    requests_per_minute: int | None = None


class BaseEmbedder(abc.ABC):
    """Base class for embedding models."""

    def __init__(self, config: ModelConfig):
        self.config = config

    @abc.abstractmethod
    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts into vectors.

        Args:
            texts: List of strings to encode.

        Returns:
            numpy array of shape (len(texts), dimensions).
        """

    @abc.abstractmethod
    def load(self) -> None:
        """Load the model into memory/GPU."""

    @abc.abstractmethod
    def unload(self) -> None:
        """Release model resources."""


class LocalEmbedder(BaseEmbedder):
    """Embedding using a local sentence-transformers model on GPU."""

    def __init__(self, config: ModelConfig):
        super().__init__(config)
        self._model = None

    def load(self) -> None:
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(
            self.config.model_id,
            device="cuda",
        )

    def encode(self, texts: list[str]) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        return self._model.encode(
            texts,
            batch_size=self.config.batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,
            convert_to_numpy=True,
            precision=self.config.precision,
        )

    def unload(self) -> None:
        if self._model is not None:
            del self._model
            self._model = None
            import torch

            torch.cuda.empty_cache()


class OpenRouterEmbedder(BaseEmbedder):
    """Embedding using OpenAI-compatible API via OpenRouter."""

    def __init__(self, config: ModelConfig):
        super().__init__(config)
        self._client = None

    def load(self) -> None:
        import os

        from openai import OpenAI

        self._client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.environ["OPENROUTER_API_KEY"],
        )

    def encode(self, texts: list[str]) -> np.ndarray:
        if self._client is None:
            raise RuntimeError("Client not initialised. Call load() first.")

        dimensions = self.config.reduced_dimensions or self.config.dimensions

        response = self._client.embeddings.create(
            model=self.config.model_id,
            input=texts,
            dimensions=dimensions,
        )

        vectors = [item.embedding for item in response.data]
        arr = np.array(vectors, dtype=np.float32)

        # L2 normalize
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        arr = arr / np.maximum(norms, 1e-12)

        return arr

    def unload(self) -> None:
        self._client = None


def create_embedder(config: ModelConfig) -> BaseEmbedder:
    """Factory function to create the appropriate embedder."""
    if config.model_type == "local":
        return LocalEmbedder(config)
    elif config.model_type == "api":
        return OpenRouterEmbedder(config)
    else:
        raise ValueError(f"Unknown model type: {config.model_type}")
