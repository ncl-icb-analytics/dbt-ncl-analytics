import yaml
import os
import sys
import re

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
SOURCES_DIR = os.path.join(PROJECT_DIR, 'models', 'sources')
# Note: Raw directory will be determined based on source mapping
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
                # Handle comparison and mathematical operators to preserve meaning
                safe_name = col_name.replace('+', '_plus')
                safe_name = safe_name.replace('<', '_lt_')
                safe_name = safe_name.replace('>', '_gt_')
                safe_name = safe_name.replace('=', '_eq_')
                safe_name = safe_name.replace('*', '_all_')
                # Remove symbols that don't have clear semantic meaning in column names
                safe_name = safe_name.replace('?', '')
                safe_name = safe_name.replace('!', '')
                safe_name = safe_name.replace('@', '_at_')
                safe_name = safe_name.replace('$', '')
                safe_name = safe_name.replace('|', '_or_')
                # Then replace dots, slashes, hyphens, spaces, and other problematic characters
                safe_name = re.sub(r'[\.\/\&\-\s\(\)\[\]]+', '_', safe_name)

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

            # Handle column names starting with digits (invalid SQL identifier)
            if safe_name and safe_name[0].isdigit():
                # Prepend the first word based on context, or use generic prefix
                # For numeric ranges like "0_6", "52_plus", prepend descriptive word
                if '_' in safe_name:
                    # Check if it looks like a range pattern (e.g., "0_6_weeks" -> "weeks_0_6")
                    parts = safe_name.split('_')
                    # If last part is a descriptive word, move it to front
                    if parts[-1].isalpha() and not parts[-1].isdigit():
                        last_word = parts[-1]
                        safe_name = f"{last_word}_{'_'.join(parts[:-1])}"
                    else:
                        # Generic prefix for other numeric-starting columns
                        safe_name = f"col_{safe_name}"
                else:
                    safe_name = f"col_{safe_name}"

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

    # Delete existing raw files
    raw_files_path = os.path.join(PROJECT_DIR, 'models', 'raw')

    for root, dirs, files in os.walk(raw_files_path):
        for file in files:
            if file.endswith(('.sql', '.yml')):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Deleted raw model: {file_path}")

    # Load all source files from sources directory
    if not os.path.exists(SOURCES_DIR):
        print(f"Error: sources directory not found at {SOURCES_DIR}", file=sys.stderr)
        print("Please run 2_generate_sources.py first to generate source files", file=sys.stderr)
        sys.exit(1)

    # Read all .yml files from sources directory
    # Sources in source_mappings.yml: Use auto-generated file (which already has manual table definitions preferred)
    # Sources NOT in source_mappings.yml: Use manual sources.yml definition
    all_sources = []
    source_files = {}  # Track which file each source came from
    manual_sources = {}  # Track manual sources by name (NOT in source_mappings.yml)
    auto_sources = {}  # Track auto-generated sources by name
    
    # Get list of source names from mappings (these are auto-generated)
    auto_generated_source_names = set(mappings.keys())
    
    # First, load manual sources.yml (these override auto-generated sources)
    manual_sources_file = os.path.join(SOURCES_DIR, 'sources.yml')
    if os.path.exists(manual_sources_file):
        with open(manual_sources_file) as f:
            sources_data = yaml.safe_load(f)
            if sources_data and 'sources' in sources_data:
                for source in sources_data['sources']:
                    source_name = source['name']
                    # Sources in sources.yml override auto-generated ones
                    manual_sources[source_name] = source
                    source_files[source_name] = 'sources.yml'
    
    # Then load auto-generated files (excluding sources.yml), but skip if already in sources.yml
    for filename in sorted(os.listdir(SOURCES_DIR)):
        if filename.endswith('.yml') and filename != 'sources.yml':
            filepath = os.path.join(SOURCES_DIR, filename)
            with open(filepath) as f:
                sources_data = yaml.safe_load(f)
                if sources_data and 'sources' in sources_data:
                    for source in sources_data['sources']:
                        source_name = source['name']
                        # Only add if not already defined in sources.yml (manual sources take precedence)
                        if source_name not in manual_sources:
                            auto_sources[source_name] = source
                            source_files[source_name] = filename
    
    # Combine all sources: manual sources (from sources.yml) take precedence, then auto-generated
    for source_name, source in manual_sources.items():
        all_sources.append(source)
    for source_name, source in auto_sources.items():
        all_sources.append(source)

    total_models = 0
    models_by_domain = {'commissioning': 0, 'olids': 0, 'shared': 0, 'phenolab': 0}

    for source in all_sources:
        source_name = source['name']
        
        # Use source name as-is for source() calls (no 'auto_' prefix)
        # The 'auto_' prefix is only in filenames, not in source names
        source_ref_name = source_name
        original_source_name = source_name  # For mapping lookup

        # Get raw prefix and domain from mappings using original source name
        if original_source_name in mappings:
            mapping = mappings[original_source_name]
            prefix = mapping.get('raw_prefix', f'raw_{original_source_name}')
            domain = mapping.get('domain', 'commissioning')  # Default to commissioning if not specified
        else:
            prefix = f'raw_{original_source_name}'
            # Set domain based on source name patterns
            if original_source_name.startswith('reference') or original_source_name == 'olids':
                domain = 'commissioning'  # Reference sources go to commissioning
            elif original_source_name.startswith('dictionary'):
                domain = 'shared'  # Dictionary sources go to shared
            else:
                domain = 'commissioning'  # Default
            print(f"Warning: No mapping found for source '{original_source_name}', using defaults (domain: {domain})")

        # Set raw directory based on new layer-first structure
        raw_dir = os.path.join(PROJECT_DIR, 'models', 'raw', domain)

        os.makedirs(raw_dir, exist_ok=True)

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
            # Use source_ref_name (prefixed for auto, original for manual) in source() call
            # But keep raw model names unchanged (using prefix from mappings)
            model_sql = f"""-- Raw layer model for {source_ref_name}.{table_name}
-- Source: {source['database']}{schema_info}
{description_comment}-- This is a 1:1 passthrough from source with standardized column names
select
    {column_list}
from {{{{ source('{source_ref_name}', '{table_name}') }}}}"""

            # Create model name with prefix and safe table name
            model_name = f"{prefix}_{table_name_safe}"
            out_path = os.path.join(raw_dir, f'{model_name}.sql')

            with open(out_path, 'w') as out_f:
                out_f.write(model_sql + '\n')

            total_models += 1
            models_by_domain[domain] += 1
            print(f"Created {domain} raw model: {model_name}.sql")

    print(f"\nTotal raw models created: {total_models}")
    print(f"Models by domain:")
    for domain, count in models_by_domain.items():
        if count > 0:
            print(f"  - {domain}: {count} models")

    print(f"\n[SUCCESS] Raw layer generation complete!")
    print(f"Next steps:")
    print(f"  1. Review generated raw models in models/raw/<domain>/")
    print(f"  2. Update dbt_project.yml to configure raw layer")
    print(f"  3. Build manually crafted staging models that reference these raw models")

if __name__ == '__main__':
    main()