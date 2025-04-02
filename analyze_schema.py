from graphql import build_schema, get_named_type, is_list_type, is_non_null_type
from typing import Dict, List


def analyze_schema(schema):
    tables = {}
    for type_name, type_obj in schema.type_map.items():
        if type_name.startswith('__') or not hasattr(type_obj, 'fields'):
            continue
        
        fields = []
        relations = []
        for field_name, field in type_obj.fields.items():
            field_type = get_named_type(field.type)
            is_list = is_list_type(field.type)
            is_required = is_non_null_type(field.type)
            
            if field_type.name in schema.type_map:  # Это связь
                relations.append({
                    'field': field_name,
                    'target': field_type.name,
                    'is_list': is_list,
                    'is_required': is_required
                })
            else:  # Обычное поле
                fields.append({
                    'name': field_name,
                    'type': field_type.name,
                    'is_required': is_required,
                    'is_list': is_list
                })
        
        tables[type_name] = {'fields': fields, 'relations': relations}
    return tables

def generate_sql(tables):
    sql_commands = []
    type_mapping = {
        'ID': 'SERIAL PRIMARY KEY',
        'String': 'TEXT',
        'Int': 'INTEGER',
        'Boolean': 'BOOLEAN',
        'Float': 'FLOAT'
    }

    # Создаём таблицы
    for table_name, data in tables.items():
        columns = []
        
        # Обычные поля
        for field in data['fields']:
            if field['is_list']:
                continue  # Массивы обрабатываем отдельно
            
            sql_type = type_mapping.get(field['type'], 'TEXT')
            nullable = '' if field['is_required'] else 'NULL'
            columns.append(f"{field['name']} {sql_type} {nullable}")

        # Первичный ключ (первое поле ID)
        if columns and 'id' in [f['name'] for f in data['fields']]:
            columns.append("PRIMARY KEY (id)")

        sql_commands.append(
            f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n);"
        )

        # Связи
        for rel in data['relations']:
            if not rel['is_list']:
                # Many-to-One
                sql_commands.append(
                    f"ALTER TABLE {table_name} "
                    f"ADD COLUMN {rel['field']}_id INTEGER "
                    f"REFERENCES {rel['target']}(id);"
                )
            else:
                # Many-to-Many (связующая таблица)
                junction_table = f"{table_name}_{rel['field']}"
                sql_commands.append(
                    f"CREATE TABLE {junction_table} (\n"
                    f"  {table_name.lower()}_id INTEGER REFERENCES {table_name}(id),\n"
                    f"  {rel['target'].lower()}_id INTEGER REFERENCES {rel['target']}(id),\n"
                    f"  PRIMARY KEY ({table_name.lower()}_id, {rel['target'].lower()}_id)\n"
                    ");"
                )

    return sql_commands

if __name__ == "__main__":
    with open("s21schema/schema/schema.gql", "r") as f:
        gql_schema = build_schema(f.read())
    import json
    
    with open("analyze_schema.json", "w") as f:
        json.dump(analyze_schema(gql_schema), f, indent=4, ensure_ascii=False)