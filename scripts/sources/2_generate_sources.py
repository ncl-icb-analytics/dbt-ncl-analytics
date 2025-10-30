import pandas as pd
import yaml
import os
import sys

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
INPUT_FILE = os.path.join(CURRENT_DIR, 'table_metadata.csv')
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'models', 'sources')
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')

def load_manual_sources(mappings):
    """Load manually defined sources and their tables from sources.yml
    
    Args:
        mappings: dict of source mappings from source_mappings.yml (to check if source is auto-generated)
    
    Returns:
        tuple: (manual_only_sources, manual_table_overrides)
            - manual_only_sources: set of source names that are fully manual (NOT in source_mappings.yml)
            - manual_table_overrides: dict {source_name: {table_name: table_def}} for manual table definitions
    """
    manual_sources_file = os.path.join(OUTPUT_DIR, 'sources.yml')
    manual_only_sources = set()  # Sources NOT in source_mappings.yml (fully manual, like 'aic')
    manual_table_overrides = {}  # {source_name: {table_name: table_def}} for sources IN source_mappings.yml
    
    # Get set of source names that are auto-generated (from source_mappings.yml)
    # mappings is keyed by (database, schema) tuples, so iterate values to get source names
    auto_generated_source_names = set()
    for mapping in mappings.values():
        if 'source_name' in mapping:
            auto_generated_source_names.add(mapping['source_name'])
    
    if os.path.exists(manual_sources_file):
        with open(manual_sources_file, 'r') as f:
            sources_data = yaml.safe_load(f)
            
            # Load fully manual sources (NOT in source_mappings.yml)
            if sources_data and 'sources' in sources_data and sources_data['sources']:
                for source in sources_data['sources']:
                    source_name = source['name']
                    
                    # Check if source is in source_mappings.yml (auto-generated) or not (fully manual)
                    if source_name not in auto_generated_source_names:
                        # Fully manual source - NOT in source_mappings.yml (e.g. 'aic')
                        manual_only_sources.add(source_name)
                        if 'tables' in source:
                            manual_table_overrides[source_name] = {}
                            for table in source['tables']:
                                table_name = table['name']
                                manual_table_overrides[source_name][table_name] = table
            
            # Load manual table overrides for sources IN source_mappings.yml
            # These are stored under 'manual_table_overrides' key to avoid dbt seeing them as duplicate sources
            if sources_data and 'manual_table_overrides' in sources_data and sources_data['manual_table_overrides']:
                for override in sources_data['manual_table_overrides']:
                    if isinstance(override, dict) and 'source_name' in override:
                        source_name = override['source_name']
                        if source_name in auto_generated_source_names:
                            # This is a table override for an auto-generated source
                            if source_name not in manual_table_overrides:
                                manual_table_overrides[source_name] = {}
                            if 'tables' in override:
                                for table in override['tables']:
                                    table_name = table['name']
                                    manual_table_overrides[source_name][table_name] = table
    
    return manual_only_sources, manual_table_overrides

def load_source_mappings():
    """Load source mappings from YAML file"""
    if not os.path.exists(MAPPINGS_FILE):
        print(f"Error: Mappings file not found at {MAPPINGS_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(MAPPINGS_FILE, 'r') as f:
        mappings_data = yaml.safe_load(f)

    # Convert to lookup dictionaries
    db_schema_to_source = {}

    for source in mappings_data['sources']:
        db = source['database']  # Preserve original case
        schema = source.get('schema', '')  # Preserve original case

        if schema:
            # Specific schema mapping - use uppercase for lookup key only
            key = (db.upper(), schema.upper())
        else:
            # Database-level mapping (all schemas)
            key = (db.upper(), '*')

        db_schema_to_source[key] = source

    return db_schema_to_source

def find_source_mapping(database, schema, mappings):
    """Find the appropriate source mapping for a database/schema combination"""
    database = database.upper()
    schema = schema.upper()

    # First try exact match
    if (database, schema) in mappings:
        return mappings[(database, schema)]

    # Then try database-level match
    if (database, '*') in mappings:
        return mappings[(database, '*')]

    return None

def get_source_filename(source_name, database, schema):
    """Generate a filename for the source based on database and schema"""
    # Normalize database name
    db_normalized = database.replace('DATA_LAKE__', 'data_lake_').replace('Dictionary', 'dictionary').lower()

    # Normalize schema name
    schema_normalized = schema.lower().replace('_', '_').replace('-', '_')

    # Create filename: auto_database_schema.yml (prefix with 'auto_' for auto-generated sources)
    if db_normalized.startswith('data_lake_ncl'):
        # For DATA_LAKE__NCL, use more specific names
        filename = f"auto_{db_normalized}_{schema_normalized}.yml"
    else:
        # For regular databases
        filename = f"auto_{db_normalized}_{schema_normalized}.yml"

    return filename

def main():
    # Load mappings
    mappings = load_source_mappings()
    print(f"Loaded {len(mappings)} source mappings from {MAPPINGS_FILE}")
    
    # Load manual sources and table overrides
    manual_only_sources, manual_table_overrides = load_manual_sources(mappings)
    if manual_only_sources:
        print(f"Found {len(manual_only_sources)} fully manual sources (will skip auto-generation): {', '.join(sorted(manual_only_sources))}")
    if manual_table_overrides:
        print(f"Found {len(manual_table_overrides)} sources with manual table overrides: {', '.join(sorted(manual_table_overrides.keys()))}")
        for source_name, tables in manual_table_overrides.items():
            print(f"  - {source_name}: {len(tables)} manually defined tables")

    # Try comma first, fallback to tab if error
    try:
        df = pd.read_csv(INPUT_FILE, sep=',')
        df.columns = df.columns.str.strip()
        print("Columns found in file (comma):", df.columns.tolist())
        if len(df.columns) == 1:
            raise ValueError("Only one column found, trying tab delimiter.")
    except Exception:
        df = pd.read_csv(INPUT_FILE, sep='\t')
        df.columns = df.columns.str.strip()
        print("Columns found in file (tab):", df.columns.tolist())

    # Group by database and schema to create sources
    sources_by_file = {}  # Track sources by output filename
    total_tables = 0

    for (database, schema), group in df.groupby(['DATABASE_NAME', 'SCHEMA_NAME']):
        # Find mapping for this database/schema
        mapping = find_source_mapping(database, schema, mappings)

        if not mapping:
            print(f"Warning: No mapping found for {database}.{schema}, skipping...")
            continue

        # Check if source is enabled
        if not mapping.get('enabled', True):  # Default to True if not specified
            print(f"Info: Source '{mapping['source_name']}' is disabled, skipping...")
            continue

        source_name = mapping['source_name']
        
        # Skip sources that are fully manual (NOT in source_mappings.yml)
        if source_name in manual_only_sources:
            print(f"Info: Source '{source_name}' is fully manual (not in source_mappings.yml), skipping auto-generation...")
            continue
        
        # Note: Source names stay as-is (no 'auto_' prefix in source name)
        # Only the filename gets the 'auto_' prefix to distinguish from manual sources

        # Determine output filename for this source (will have 'auto_' prefix)
        filename = get_source_filename(source_name, database, schema)

        # Initialize file entry if needed
        if filename not in sources_by_file:
            sources_by_file[filename] = []

        tables = []
        manual_source_tables = manual_table_overrides.get(source_name, {})

        # Get all tables for this source from metadata
        for table_name, table_group in group.groupby('TABLE_NAME'):
            # Check if specific tables are configured
            if 'tables' in mapping and table_name not in mapping['tables']:
                continue

            # Prefer manual table definition if it exists (from sources.yml table overrides)
            if source_name in manual_table_overrides and table_name in manual_source_tables:
                print(f"  Using manual definition for {source_name}.{table_name}")
                tables.append(manual_source_tables[table_name])
            else:
                # Auto-generate from metadata
                sorted_columns = table_group.sort_values('ORDINAL_POSITION')

                table = {
                    'name': table_name,
                    'identifier': f'"{table_name}"',  # Quote identifier to preserve case
                    'columns': [{'name': col, 'data_type': dtype}
                               for col, dtype in zip(sorted_columns['COLUMN_NAME'],
                                                   sorted_columns['DATA_TYPE'])]
                }
                tables.append(table)

        source = {
            'name': source_name,  # Keep original source name (no 'auto_' prefix)
            'database': f'"{mapping["database"]}"',  # Quote for case sensitivity
            'schema': f'"{mapping.get("schema", schema)}"',  # Quote schema and use mapped case
            'description': mapping.get('description', ''),
            'tables': tables
        }

        sources_by_file[filename].append(source)
        total_tables += len(tables)
        print(f"Created source '{source_name}' with {len(tables)} tables -> {filename}")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write each source file (excluding sources.yml which contains manual-only sources)
    for filename, sources in sources_by_file.items():
        # Skip sources.yml - this file contains manually defined sources that are NOT in source_mappings.yml
        if filename == 'sources.yml':
            print(f"  Skipping {filename} (manual sources file)")
            continue
            
        sources_yml = {
            'version': 2,
            'sources': sources
        }

        output_path = os.path.join(OUTPUT_DIR, filename)
        with open(output_path, 'w') as f:
            yaml.dump(sources_yml, f, sort_keys=False, default_flow_style=False)

        print(f"  Written: {filename}")
    
    # Note: Manual sources.yml should only contain sources NOT in source_mappings.yml
    # Sources in source_mappings.yml will be auto-generated with all tables (preferring manual table definitions)

    print(f"\nGenerated {len(sources_by_file)} source files in {OUTPUT_DIR}")
    print(f"Total: {sum(len(sources) for sources in sources_by_file.values())} sources with {total_tables} tables")
    print(f"\nNext step: Generate raw layer models:")
    print(f"  python scripts/sources/3_generate_raw_models.py")

if __name__ == '__main__':
    main()