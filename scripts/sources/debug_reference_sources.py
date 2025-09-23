#!/usr/bin/env python3
"""
Debug script to check why DATA_LAKE__NCL reference sources aren't being created
"""

import pandas as pd
import yaml
import os

# Load metadata
df = pd.read_csv('scripts/sources/table_metadata.csv')
print(f"Total rows in metadata: {len(df)}")

# Filter for DATA_LAKE__NCL
reference_data = df[df['DATABASE_NAME'] == 'DATA_LAKE__NCL']
print(f"DATA_LAKE__NCL rows: {len(reference_data)}")

if len(reference_data) > 0:
    print("\nFirst 5 DATA_LAKE__NCL entries:")
    print(reference_data.head())

    print(f"\nUnique schemas in DATA_LAKE__NCL:")
    print(reference_data['SCHEMA_NAME'].unique())

    print(f"\nTable count by schema:")
    print(reference_data.groupby('SCHEMA_NAME')['TABLE_NAME'].nunique())

# Load source mappings
with open('scripts/sources/source_mappings.yml', 'r') as f:
    mappings_data = yaml.safe_load(f)

print(f"\nSource mappings loaded: {len(mappings_data['sources'])}")

# Find reference mapping
reference_mapping = None
for source in mappings_data['sources']:
    if source['database'] == 'DATA_LAKE__NCL':
        reference_mapping = source
        break

if reference_mapping:
    print(f"\nFound reference mapping:")
    print(f"  source_name: {reference_mapping['source_name']}")
    print(f"  database: {reference_mapping['database']}")
    print(f"  schema: {reference_mapping.get('schema', 'NOT SPECIFIED')}")
else:
    print("\nNo reference mapping found for DATA_LAKE__NCL!")

# Check how lookup would work
db_schema_to_source = {}
for source in mappings_data['sources']:
    db = source['database']
    schema = source.get('schema', '')

    if schema:
        key = (db.upper(), schema.upper())
    else:
        key = (db.upper(), '*')

    db_schema_to_source[key] = source

print(f"\nMapping keys:")
for key in db_schema_to_source.keys():
    if 'DATA_LAKE__NCL' in key[0]:
        print(f"  {key}")

# Test lookup for each DATA_LAKE__NCL schema
if len(reference_data) > 0:
    print(f"\nTesting lookup for each schema:")
    for schema in reference_data['SCHEMA_NAME'].unique():
        # Try exact match
        exact_key = ('DATA_LAKE__NCL', schema)
        wildcard_key = ('DATA_LAKE__NCL', '*')

        exact_match = exact_key in db_schema_to_source
        wildcard_match = wildcard_key in db_schema_to_source

        print(f"  {schema}: exact={exact_match}, wildcard={wildcard_match}")