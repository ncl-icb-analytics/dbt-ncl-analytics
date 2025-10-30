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

def load_manual_sources():
    """Load manually defined sources and tables from sources.yml
    
    Returns:
        tuple: (set of source names, dict mapping source_name -> set of table names)
    """
    manual_sources_file = os.path.join(OUTPUT_DIR, 'sources.yml')
    manual_source_names = set()
    manual_source_tables = {}  # source_name -> set of table names
    
    if os.path.exists(manual_sources_file):
        with open(manual_sources_file, 'r') as f:
            sources_data = yaml.safe_load(f)
            if sources_data and 'sources' in sources_data and sources_data['sources']:
                for source in sources_data['sources']:
                    source_name = source['name']
                    manual_source_names.add(source_name)
                    
                    # Track manually defined tables for this source
                    if 'tables' in source:
                        table_names = {table['name'] for table in source['tables']}
                        manual_source_tables[source_name] = table_names
    
    return manual_source_names, manual_source_tables

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
    
    # Load manually defined sources and tables from sources.yml (these override auto-generation)
    manual_source_names, manual_source_tables = load_manual_sources()
    if manual_source_names:
        print(f"Found {len(manual_source_names)} manually defined sources in sources.yml (will skip auto-generation): {', '.join(sorted(manual_source_names))}")

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
        
        # Skip sources that are manually defined in sources.yml (they override auto-generation)
        if source_name in manual_source_names:
            print(f"Info: Source '{source_name}' is manually defined in sources.yml, skipping auto-generation...")
            continue
        
        # Note: Source names stay as-is (no 'auto_' prefix in source name)
        # Only the filename gets the 'auto_' prefix to distinguish from manual sources

        # Determine output filename for this source (will have 'auto_' prefix)
        filename = get_source_filename(source_name, database, schema)

        # Initialize file entry if needed
        if filename not in sources_by_file:
            sources_by_file[filename] = []

        tables = []

        # Get all tables for this source from metadata
        for table_name, table_group in group.groupby('TABLE_NAME'):
            # Check if specific tables are configured
            if 'tables' in mapping and table_name not in mapping['tables']:
                continue

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

    # Clean up auto-generated files: remove sources that are now manually defined
    # Find all auto-generated files and remove manual sources from them
    if os.path.exists(OUTPUT_DIR):
        for filename in os.listdir(OUTPUT_DIR):
            if filename.startswith('auto_') and filename.endswith('.yml'):
                file_path = os.path.join(OUTPUT_DIR, filename)
                try:
                    with open(file_path, 'r') as f:
                        file_sources = yaml.safe_load(f)
                        if file_sources and 'sources' in file_sources:
                            # Filter out manual sources
                            original_sources = file_sources['sources']
                            filtered_sources = [
                                source for source in original_sources
                                if source.get('name') not in manual_source_names
                            ]
                            
                            # If any sources were removed, update or delete the file
                            if len(filtered_sources) < len(original_sources):
                                removed_sources = [
                                    source.get('name') for source in original_sources
                                    if source.get('name') in manual_source_names
                                ]
                                
                                if filtered_sources:
                                    # Some sources remain, update the file
                                    file_sources['sources'] = filtered_sources
                                    with open(file_path, 'w') as f:
                                        yaml.dump(file_sources, f, sort_keys=False, default_flow_style=False)
                                    print(f"  Updated {filename} (removed manual sources: {', '.join(removed_sources)})")
                                else:
                                    # All sources were manual, delete the file
                                    os.remove(file_path)
                                    print(f"  Deleted {filename} (all sources [{', '.join(removed_sources)}] are now manually defined in sources.yml)")
                except Exception as e:
                    print(f"  Warning: Could not check {filename}: {e}")

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