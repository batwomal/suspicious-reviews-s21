import re

def process_sql_file(input_file, output_file):
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Регулярное выражение для поиска ADD COLUMN
    pattern = re.compile(
        r'(ALTER TABLE\s+"([^"]+)"\s+ADD COLUMN\s+"([^"]+)"[^;]+;)',
        re.IGNORECASE
    )
    
    # Заменяем каждый ADD COLUMN на DROP + ADD
    def replace_match(match):
        full_statement = match.group(1)
        table_name = match.group(2)
        column_name = match.group(3)
        
        drop_statement = f'ALTER TABLE "{table_name}" DROP COLUMN IF EXISTS "{column_name}";\n'
        return drop_statement + full_statement
    
    new_content = pattern.sub(replace_match, content)
    
    with open(output_file, 'w') as f:
        f.write(new_content)

# Использование
process_sql_file('02_relations.sql', '02_relations_with_drop.sql')