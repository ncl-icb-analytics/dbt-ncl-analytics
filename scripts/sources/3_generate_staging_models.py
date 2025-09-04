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

def sanitise_column_name(col_name):
    """Convert column name to SQL-safe identifier"""
    # Replace dots, slashes, hyphens, spaces, and other problematic characters
    safe_name = re.sub(r'[\.\/\&\-\s\(\)\[\]]+', '_', col_name)
    # Remove multiple consecutive underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    # Remove leading/trailing underscores
    safe_name = safe_name.strip('_')
    
    # Deal with camel case including starting with acronyms
    # First handle the special "of" cases (both "Of" and "of") after acronyms
    safe_name = re.sub(r'([A-Z]+)([Oo]f)([A-Z])', r'\1_\2_\3', safe_name)
    # Add underscores before uppercase letters that follow lowercase letters or numbers
    safe_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', safe_name)
    # Handle acronyms followed by PascalCase (like GPPractice -> GP_Practice, CCG -> CCG)
    # This splits when multiple capitals are followed by a capital then lowercase
    safe_name = re.sub(r'([A-Z])([A-Z]+)([A-Z][a-z])', r'\1\2_\3', safe_name)
    # Convert to lowercase
    safe_name = safe_name.lower()

    # Consistent pseudo key naming  - currently removed due to ambiguous pseudo renaming (2+ pseudo keys identified)
    # if 'pseudo' in safe_name.lower() and 'nhs_number' in safe_name.lower():
    #    return 'sk_patient_id'
    
    # Handle reserved words and ensure valid SQL identifier
    if safe_name.lower() in ['pseudo', 'group', 'order', 'having', 'where']:
        safe_name = f'{safe_name}_value'
    
    # Convert to lowercase for consistency
    return safe_name.lower()

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
            domain = 'commissioning'  # Default
            print(f"Warning: No mapping found for source '{source_name}', using defaults")
        
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

            # Quote source columns and use safe column names
            column_mappings = []
            for col in columns:
                safe_col = sanitise_column_name(col)
                column_mappings.append(f'"{col}" as {safe_col}')
            
            column_list = ',\n    '.join(column_mappings)
            
            # Add description if available
            description_comment = ""
            if source.get('description'):
                description_comment = f"-- Description: {source.get('description')}\n"
            
            model_sql = f"""-- Staging model for {source_name}.{table_name}
-- Source: {source['database']}.{source['schema']}
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
    
    print(f"\n✅ Staging model generation complete!")
    print(f"Next steps:")
    print(f"  1. Review generated staging models in models/<domain>/staging/")
    print(f"  2. Run dbt to test the models: dbt run --select staging")
    print(f"  3. Begin building your transformation models in modelling/ directories")

if __name__ == '__main__':
    main()