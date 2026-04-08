import pandas as pd

# Read input file with unit-separator delimiter
input_file = 'data/inputs/new_unified_dbo_qry_eom.csv'
input_df = pd.read_csv(input_file, sep=chr(0x1F), dtype=str)

# Read entity mappings
mappings_df = pd.read_csv('data/inputs/mappings/entity_mappings.csv', dtype=str)

print(f'[INFO] Input file: {len(input_df)} rows')
print(f'[INFO] Mappings file: {len(mappings_df)} rows')
print()

# For each row in input, lookup the Entity_Name in mappings
def get_mapping_label(entity_name):
    # Look for this entity in mappings
    matches = mappings_df[mappings_df['Entity'] == entity_name]
    
    if len(matches) == 0:
        return 'UNMAPPED'
    
    # Get the first match (entities can have multiple mappings)
    first_match = matches.iloc[0]
    region = first_match['Region'].strip() if pd.notna(first_match['Region']) else 'Unknown'
    channel = first_match['Channel_Level'].strip() if pd.notna(first_match['Channel_Level']) else 'Direct'
    
    # Format as 'Region - Channel_Level'
    label = f'{region} - {channel}'
    return label

# Apply mapping
input_df['mapping_label'] = input_df['Entity_Name'].apply(get_mapping_label)

# Show some examples
print('Sample mappings:')
sample = input_df[['Entity_Name', 'Region', 'Net_Value', 'mapping_label']].head(15)
print(sample.to_string(index=False))
print()

# Write output
output_file = 'data/inputs/new_unified_dbo_qry_eom_with_mapping_labels.csv'
input_df.to_csv(output_file, sep=',', index=False, encoding='utf-8')

print(f'[OK] Output file created: {output_file}')
print(f'[OK] Rows: {len(input_df)}')
print(f'[OK] Unique mapping labels: {input_df["mapping_label"].nunique()}')
print()
print('Mapping label distribution:')
dist = input_df['mapping_label'].value_counts()
print(dist.to_string())
