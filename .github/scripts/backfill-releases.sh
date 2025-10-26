#!/bin/bash
# Backfills releases from v1.0.0 commit to current HEAD based on conventional commits

set -e

# Starting commit (layer restructure) - this will be v1.0.0
V1_COMMIT="8f99af1"
CURRENT_VERSION="1.0.0"

echo "Starting from v1.0.0 at commit $V1_COMMIT"
echo ""

# Get all merge commits (PRs) since v1.0.0
MERGE_COMMITS=$(git log --merges --first-parent main --format="%H" ${V1_COMMIT}..HEAD | tac)

# Function to determine version bump from commits
get_version_bump() {
    local prev_commit=$1
    local current_commit=$2

    # Get all commits between these two points
    local commits=$(git log --format="%s" ${prev_commit}..${current_commit})

    # Check for breaking changes
    if echo "$commits" | grep -qi "BREAKING CHANGE"; then
        echo "major"
        return
    fi

    # Check for features (new functionality)
    if echo "$commits" | grep -qiE "^feat(\(|:)|^feature:"; then
        echo "minor"
        return
    fi

    # Default to patch for fixes, docs, refactors, etc.
    echo "patch"
}

# Function to increment version
increment_version() {
    local version=$1
    local bump=$2

    IFS='.' read -r major minor patch <<< "$version"

    case $bump in
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
    esac

    echo "${major}.${minor}.${patch}"
}

# Track previous commit for comparison
PREV_COMMIT=$V1_COMMIT

# Array to store version information
declare -a VERSIONS

# Add v1.0.0 as the base
VERSIONS+=("v1.0.0:$V1_COMMIT:Layer-based folder structure")

echo "Analysing merge commits to determine version bumps..."
echo ""

# Process each merge commit
for commit in $MERGE_COMMITS; do
    # Get commit message
    MSG=$(git log -1 --format="%s" $commit)

    # Determine version bump
    BUMP=$(get_version_bump "$PREV_COMMIT" "$commit")

    # Increment version
    CURRENT_VERSION=$(increment_version "$CURRENT_VERSION" "$BUMP")

    # Store version info
    VERSIONS+=("v${CURRENT_VERSION}:${commit}:${MSG}")

    echo "  $commit → v$CURRENT_VERSION ($BUMP): $MSG"

    PREV_COMMIT=$commit
done

echo ""
echo "Generated ${#VERSIONS[@]} versions"
echo ""
echo "Would create the following tags:"
echo ""

for version_info in "${VERSIONS[@]}"; do
    IFS=':' read -r version commit message <<< "$version_info"
    echo "  $version at $commit - $message"
done

echo ""
read -p "Proceed with creating these tags? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Create tags
echo "Creating tags..."
for version_info in "${VERSIONS[@]}"; do
    IFS=':' read -r version commit message <<< "$version_info"

    # Get commit date for backdating
    COMMIT_DATE=$(git log -1 --format="%aD" $commit)

    # Create annotated tag with original date
    GIT_COMMITTER_DATE="$COMMIT_DATE" git tag -a "$version" $commit -m "$message"

    echo "  Created $version"
done

echo ""
echo "Tags created successfully!"
echo ""
echo "Next steps:"
echo "1. Review tags: git tag -l"
echo "2. Push tags: git push origin --tags"
echo "3. Manually create GitHub releases from tags with release notes"
