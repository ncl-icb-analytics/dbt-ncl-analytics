import yaml
import os
import sys
import re

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
SOURCES_YML = os.path.join(PROJECT_DIR, 'models', 'sources.yml')
# Note: Staging directory will be determined based on source mapping
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')

def load_source_mappings():
    """Load source mappings from YAML file"""
    if not os.path.exists(MAPPINGS_FILE):
        print(f"Warning: Mappings file not found at {MAPPINGS_FILE}", file=sys.stderr)
        return {}

    with open(MAPPINGS_FILE, 'r') as f:
        mappings_data = yaml.safe_load(f)

    # Create lookup by source_name
    mappings_by_name = {}
    for source in mappings_data['sources']:
        mappings_by_name[source['source_name']] = source

    return mappings_by_name

def sanitise_filename(name):
    """Convert table name to safe filename by replacing special characters"""
    # Replace problematic characters with underscores
    safe_name = re.sub(r'[&\-\.\s]+', '_', name)
    # Remove multiple consecutive underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    # Remove leading/trailing underscores
    safe_name = safe_name.strip('_')
    return safe_name.lower()

def sanitise_column_name(col_name, apply_transformations=True, used_names=None):
    """Convert column name to SQL-safe identifier, ensuring uniqueness"""
    if used_names is None:
        used_names = set()

    # Handle special characters that can't be used as SQL identifiers
    if col_name == '%':
        base_name = 'percent'
    elif col_name == '#':
        base_name = 'number'
    elif col_name == '&':
        base_name = 'and'
    else:
        # If not applying transformations, just handle basic safety
        if not apply_transformations:
            # For unquoted identifiers, replace spaces and dots with underscores but keep simple
            safe_name = col_name.lower().replace(' ', '_').replace('.', '_')
            # Handle reserved words
            if safe_name in ['pseudo', 'group', 'order', 'having', 'where']:
                safe_name = f'{safe_name}_value'
            base_name = safe_name
        else:
            # Apply full transformations for quoted identifiers

            # Special handling for columns with ellipsis (... in cancer data)
            # These represent grouped metrics, preserve the full context
            if '...' in col_name:
                # Split by ellipsis to get the metric and the context
                parts = col_name.split('...')
                if len(parts) == 2:
                    # e.g. "DaysWithin24...AccountableProvider.24.day.wait"
                    # becomes "days_within24_accountable_provider_24_day_wait"
                    safe_name = parts[0] + '_' + parts[1].replace('.', '_')
                else:
                    safe_name = col_name.replace('...', '_')
            else:
                # Replace dots, slashes, hyphens, spaces, and other problematic characters
                safe_name = re.sub(r'[\.\/\&\-\s\(\)\[\]]+', '_', col_name)

            # Remove multiple consecutive underscores
            safe_name = re.sub(r'_+', '_', safe_name)
            # Remove leading/trailing underscores
            safe_name = safe_name.strip('_')

            # Deal with camel case including starting with acronyms

            # Step 1: Handle special case of "Of" or "of" after acronyms
            # Matches: CCGof or CCGOf followed by uppercase letter
            # Example: "CCGofResidence" -> "CCG_of_Residence"
            safe_name = re.sub(r'([A-Z]+)([Oo]f)([A-Z])', r'\1_\2_\3', safe_name)

            # Step 2: Add underscore when transitioning from lowercase/digit to uppercase
            # Matches: any lowercase letter or digit followed by uppercase letter
            # Example: "dmicDerived" -> "dmic_Derived"
            safe_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', safe_name)

            # Step 3: Split acronyms when followed by PascalCase word
            # Matches: One or more capitals, followed by another capital and then lowercase
            # The last capital is the start of the new word, so we split before it
            # Example: "GPPractice" -> "GP_Practice", "CCG" stays as "CCG"
            safe_name = re.sub(r'([A-Z])([A-Z]+)([A-Z][a-z])', r'\1\2_\3', safe_name)

            # Step 4: Convert everything to lowercase
            safe_name = safe_name.lower()

            # Handle reserved words and ensure valid SQL identifier
            if safe_name.lower() in ['pseudo', 'group', 'order', 'having', 'where']:
                safe_name = f'{safe_name}_value'

            base_name = safe_name

    # Ensure uniqueness by adding suffix if needed
    final_name = base_name
    counter = 1
    while final_name in used_names:
        final_name = f"{base_name}_{counter}"
        counter += 1

    used_names.add(final_name)
    return final_name

def main():
    # Load source mappings
    mappings = load_source_mappings()

    # Load sources.yml
    if not os.path.exists(SOURCES_YML):
        print(f"Error: sources.yml not found at {SOURCES_YML}", file=sys.stderr)
        print("Please run 2_generate_sources.py first to generate sources.yml", file=sys.stderr)
        sys.exit(1)

    with open(SOURCES_YML) as f:
        sources = yaml.safe_load(f)

    total_models = 0
    models_by_domain = {'commissioning': 0, 'olids': 0, 'shared': 0}

    for source in sources['sources']:
        source_name = source['name']

        # Get staging prefix and domain from mappings
        if source_name in mappings:
            mapping = mappings[source_name]
            prefix = mapping.get('staging_prefix', f'stg_{source_name}')
            domain = mapping.get('domain', 'commissioning')  # Default to commissioning if not specified
        else:
            prefix = f'stg_{source_name}'
            # Set domain based on source name patterns
            if source_name.startswith('reference') or source_name == 'olids':
                domain = 'commissioning'  # Reference sources go to commissioning
            elif source_name.startswith('dictionary'):
                domain = 'shared'  # Dictionary sources go to shared
            else:
                domain = 'commissioning'  # Default
            print(f"Warning: No mapping found for source '{source_name}', using defaults (domain: {domain})")

        # Set staging directory based on domain
        if domain == 'shared':
            staging_dir = os.path.join(PROJECT_DIR, 'models', 'shared', 'staging')
        else:
            staging_dir = os.path.join(PROJECT_DIR, 'models', domain, 'staging')

        os.makedirs(staging_dir, exist_ok=True)

        for table in source['tables']:
            # Keep original case for source reference
            table_name = table['name']
            # Use sanitised name for file names
            table_name_safe = sanitise_filename(table_name)
            columns = [col['name'] for col in table.get('columns', [])]
            if not columns:
                continue  # Skip tables with no columns listed

            # Column mappings with uniqueness tracking
            column_mappings = []
            used_names = set()

            for col in columns:
                # All columns need quoting and safe transformations
                safe_col = sanitise_column_name(col, apply_transformations=True, used_names=used_names)
                column_mappings.append(f'"{col}" as {safe_col}')

            column_list = ',\n    '.join(column_mappings)

            # Add description if available
            description_comment = ""
            if source.get('description'):
                description_comment = f"-- Description: {source.get('description')}\n"

            schema_info = f".{source['schema']}" if 'schema' in source else ""
            model_sql = f"""-- Staging model for {source_name}.{table_name}
-- Source: {source['database']}{schema_info}
{description_comment}
select
    {column_list}
from {{{{ source('{source_name}', '{table_name}') }}}}"""

            # Create model name with prefix and safe table name
            model_name = f"{prefix}_{table_name_safe}"
            out_path = os.path.join(staging_dir, f'{model_name}.sql')

            with open(out_path, 'w') as out_f:
                out_f.write(model_sql + '\n')

            total_models += 1
            models_by_domain[domain] += 1
            print(f"Created {domain} staging model: {model_name}.sql")

    print(f"\nTotal staging models created: {total_models}")
    print(f"Models by domain:")
    for domain, count in models_by_domain.items():
        if count > 0:
            print(f"  - {domain}: {count} models")

    print(f"\n[SUCCESS] Staging model generation complete!")
    print(f"Next steps:")
    print(f"  1. Review generated staging models in models/<domain>/staging/")
    print(f"  2. Run dbt to test the models: dbt run --select staging")
    print(f"  3. Begin building your transformation models in modelling/ directories")

if __name__ == '__main__':
    main()