"""FHIR terminology server client for concept lookup and hierarchy retrieval."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ConceptContext:
    """Rich context about a SNOMED CT concept from the FHIR terminology server."""

    concept_id: str
    display: str
    fully_specified_name: str | None = None
    synonyms: list[str] = field(default_factory=list)
    parents: list[dict[str, str]] = field(default_factory=list)  # [{code, display}]
    children: list[dict[str, str]] = field(default_factory=list)
    relationships: list[dict[str, str]] = field(default_factory=list)
    reference_sets: list[str] = field(default_factory=list)


class FHIRTerminologyClient:
    """Client for interacting with a FHIR terminology server (Ontoserver, Snowstorm, etc)."""

    SNOMED_SYSTEM = "http://snomed.info/sct"

    def __init__(self, base_url: str, auth_token: str | None = None, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        headers = {"Accept": "application/fhir+json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self._client = httpx.Client(base_url=self.base_url, headers=headers, timeout=timeout)

    def lookup(self, code: str, properties: list[str] | None = None) -> dict:
        """$lookup a SNOMED CT concept.

        Args:
            code: SNOMED CT concept ID.
            properties: List of properties to request (parent, child, etc).

        Returns:
            FHIR Parameters resource as dict.
        """
        params = {
            "system": self.SNOMED_SYSTEM,
            "code": code,
        }
        if properties:
            params["property"] = properties

        response = self._client.get("/CodeSystem/$lookup", params=params)
        response.raise_for_status()
        return response.json()

    def get_concept_context(self, code: str) -> ConceptContext:
        """Get rich context for a SNOMED concept including hierarchy.

        Args:
            code: SNOMED CT concept ID.

        Returns:
            ConceptContext with display, synonyms, parents, children, relationships.
        """
        result = self.lookup(code, properties=["parent", "child", "designation"])

        context = ConceptContext(concept_id=code, display="")

        for param in result.get("parameter", []):
            name = param.get("name")
            if name == "display":
                context.display = param.get("valueString", "")
            elif name == "designation":
                # Extract synonyms and FSN from designations
                parts = {p["name"]: p for p in param.get("part", [])}
                use_code = parts.get("use", {}).get("valueCoding", {}).get("code", "")
                value = parts.get("value", {}).get("valueString", "")
                if use_code == "900000000000003001":  # FSN
                    context.fully_specified_name = value
                elif value:
                    context.synonyms.append(value)
            elif name == "property":
                parts = {p["name"]: p for p in param.get("part", [])}
                prop_code = parts.get("code", {}).get("valueCode", "")
                prop_value = parts.get("value", {})

                if prop_code == "parent":
                    code_val = prop_value.get("valueCode", "")
                    if code_val:
                        context.parents.append({"code": code_val, "display": ""})
                elif prop_code == "child":
                    code_val = prop_value.get("valueCode", "")
                    if code_val:
                        context.children.append({"code": code_val, "display": ""})

        return context

    def search_concepts(self, term: str, count: int = 20) -> list[dict[str, str]]:
        """Search for SNOMED concepts by term using ValueSet $expand.

        Args:
            term: Search term.
            count: Maximum number of results.

        Returns:
            List of dicts with 'code' and 'display'.
        """
        params = {
            "url": f"{self.SNOMED_SYSTEM}?fhir_vs",
            "filter": term,
            "count": count,
        }

        response = self._client.get("/ValueSet/$expand", params=params)
        response.raise_for_status()

        result = response.json()
        concepts = []
        for item in result.get("expansion", {}).get("contains", []):
            concepts.append(
                {
                    "code": item.get("code", ""),
                    "display": item.get("display", ""),
                }
            )

        return concepts

    def check_subsumption(self, code_a: str, code_b: str) -> str | None:
        """Check subsumption relationship between two concepts.

        Returns:
            'subsumes', 'subsumed-by', 'equivalent', or 'not-subsumed'.
        """
        params = {
            "system": self.SNOMED_SYSTEM,
            "codeA": code_a,
            "codeB": code_b,
        }

        response = self._client.get("/CodeSystem/$subsumes", params=params)
        response.raise_for_status()

        result = response.json()
        for param in result.get("parameter", []):
            if param.get("name") == "outcome":
                return param.get("valueCode")

        return None

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()
