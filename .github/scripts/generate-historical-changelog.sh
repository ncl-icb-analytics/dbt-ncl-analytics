#!/bin/bash
# Generates a historical CHANGELOG.md from v1.0.0 to current HEAD

set -e

V1_COMMIT="8f99af1"
OUTPUT_FILE="CHANGELOG.md"

echo "# Changelog" > $OUTPUT_FILE
echo "" >> $OUTPUT_FILE
echo "All notable changes to this project are documented in this file." >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE
echo "This changelog follows [Conventional Commits](https://www.conventionalcommits.org/)." >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE
echo "## [Unreleased]" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE
echo "Changes since the last release will appear here." >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

# Read versions from tags (assumes tags are already created)
# Sort in reverse order (newest first)
TAGS=$(git tag -l "v*" --sort=-version:refname)

for tag in $TAGS; do
    # Get tag date
    TAG_DATE=$(git log -1 --format=%ai $tag | cut -d' ' -f1)

    # Get commit range for this version
    PREV_TAG=$(git describe --tags --abbrev=0 $tag^ 2>/dev/null || echo "$V1_COMMIT")

    echo "## [$tag] - $TAG_DATE" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE

    # Group commits by type
    FEATURES=$(git log --format="%s" ${PREV_TAG}..${tag} | grep -iE "^feat(\(|:)" || true)
    FIXES=$(git log --format="%s" ${PREV_TAG}..${tag} | grep -iE "^fix(\(|:)" || true)
    REFACTORS=$(git log --format="%s" ${PREV_TAG}..${tag} | grep -iE "^refactor(\(|:)" || true)
    DOCS=$(git log --format="%s" ${PREV_TAG}..${tag} | grep -iE "^docs(\(|:)" || true)

    # Output features
    if [ -n "$FEATURES" ]; then
        echo "### Features" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
        echo "$FEATURES" | while read -r line; do
            echo "- $line" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    fi

    # Output fixes
    if [ -n "$FIXES" ]; then
        echo "### Fixes" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
        echo "$FIXES" | while read -r line; do
            echo "- $line" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    fi

    # Output refactors
    if [ -n "$REFACTORS" ]; then
        echo "### Refactoring" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
        echo "$REFACTORS" | while read -r line; do
            echo "- $line" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    fi

    # Output docs
    if [ -n "$DOCS" ]; then
        echo "### Documentation" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
        echo "$DOCS" | while read -r line; do
            echo "- $line" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    fi

    # Add model change summary
    echo "### Models Changed" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
    chmod +x .github/scripts/analyse-dbt-changes.sh
    ANALYSIS=$(.github/scripts/analyse-dbt-changes.sh $tag 2>/dev/null || echo "No model changes detected")
    echo "$ANALYSIS" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
done

echo "CHANGELOG.md generated successfully!"
