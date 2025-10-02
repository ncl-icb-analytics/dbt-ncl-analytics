#!/usr/bin/env python3
"""
Master script to run all source generation steps in order
Generates metadata queries, extracts metadata, creates sources, and generates raw layer models
"""

import subprocess
import sys
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name: str) -> bool:
    """Run a Python script and return success status"""
    script_path = Path(__file__).parent / script_name

    logger.info(f"Running {script_name}...")

    try:
        # Run from project root directory, not scripts/sources directory
        project_root = Path(__file__).parent.parent.parent
        result = subprocess.run([sys.executable, str(script_path)],
                              capture_output=True, text=True, cwd=project_root, env=os.environ.copy())

        if result.returncode == 0:
            logger.info(f"✓ {script_name} completed successfully")
            if result.stdout.strip():
                logger.info(f"Output: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"✗ {script_name} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
            if result.stdout:
                logger.error(f"Output: {result.stdout}")
            return False

    except Exception as e:
        logger.error(f"✗ Failed to run {script_name}: {str(e)}")
        return False

def main():
    """Run all source generation scripts in order"""
    scripts = [
        "1a_generate_metadata_query.py",
        "1b_extract_metadata.py",
        "2_generate_sources.py",
        "3_generate_raw_models.py"
    ]

    logger.info("Starting full source generation pipeline...")

    for i, script in enumerate(scripts, 1):
        logger.info(f"Step {i}/{len(scripts)}: {script}")

        success = run_script(script)

        if not success:
            logger.error(f"Pipeline failed at step {i}. Stopping execution.")
            sys.exit(1)

    logger.info("🎉 All source generation scripts completed successfully!")
    logger.info("Generated:")
    logger.info("  - Metadata queries")
    logger.info("  - Source YAML files")
    logger.info("  - Raw layer models")

if __name__ == "__main__":
    main()