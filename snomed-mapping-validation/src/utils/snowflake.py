"""Snowflake connection helpers for exporting concept map and reference data."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ANALYST_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MODELLING"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def export_concept_map(output_path: Path) -> int:
    """Export the OLIDS concept map to parquet.

    Returns:
        Number of rows exported.
    """
    query = """
    SELECT
        cm.source_code_id,
        cm.source_code,
        cm.source_display,
        cm.source_system,
        cm.target_code_id,
        cm.target_code,
        cm.target_display,
        cm.target_system,
        cm.equivalence,
        cm.is_primary,
        cm.is_active,
        cm.concept_map_url,
        cm.concept_map_version
    FROM MODELLING.DBT_STAGING.stg_olids_concept_map cm
    WHERE cm.is_active = TRUE
    """

    conn = get_snowflake_connection()
    try:
        logger.info("Exporting concept map from Snowflake...")
        df = pd.read_sql(query, conn)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("Exported %d concept map rows to %s", len(df), output_path)
        return len(df)
    finally:
        conn.close()


def export_snomed_descriptions(output_path: Path) -> int:
    """Export all SNOMED CT descriptions from the reference terminology tables.

    This pulls from the Dictionary.Snomed schema or DATA_LAKE__NCL.TERMINOLOGY
    depending on what's available.

    Returns:
        Number of rows exported.
    """
    # This query will need to be adapted based on the actual table structure
    # in your Snowflake environment. The TRUD RF2 import typically creates
    # tables like sct2_Description_Snapshot_*.
    query = """
    SELECT
        d.id AS description_id,
        d.conceptid AS concept_id,
        d.term,
        d.typeid AS type_id,
        d.languagecode AS language_code,
        d.active,
        d.moduleid AS module_id
    FROM DATA_LAKE__NCL.TERMINOLOGY.sct2_description d
    WHERE d.active = 1
      AND d.languagecode = 'en'
    """

    conn = get_snowflake_connection()
    try:
        logger.info("Exporting SNOMED descriptions from Snowflake...")
        df = pd.read_sql(query, conn)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("Exported %d SNOMED descriptions to %s", len(df), output_path)
        return len(df)
    finally:
        conn.close()
