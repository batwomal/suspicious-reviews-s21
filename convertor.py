import re
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import json

def parse_op_line(line: str, current_description: List[str]) -> Dict:
    # Парсим строку операции
    op_match = re.match(r"(\w+)\(([^)]*)\):\s*(!?\[?[\w\!\]\s]+)", line)
    if op_match:
        op_name = op_match.group(1)
        params_str = op_match.group(2)
        returns = op_match.group(3).replace('!', '').replace('[', '').replace(']', '')
        
        # Парсим параметры
        params_dict = {}
        for param in params_str.split(','):
            param = param.strip()
            if not param:
                continue
            name_type = param.split(':')
            if len(name_type) == 2:
                param_name = name_type[0].strip()
                param_type = name_type[1].strip().replace('!', '')
                params_dict[param_name] = param_type
        
        return {
            "operationName": op_name,
            "params": params_dict,
            "returns": returns,
            "description": ' '.join(current_description).strip() if current_description else None
        }

def parse_description(lines: List[str], current_line_idx: int) -> Tuple[str, int]:
    """Парсит многострочное или однострочное описание и возвращает текст описания и новый индекс строки"""
    if current_line_idx >= len(lines):
        return "", current_line_idx

    text = "\n".join(lines[current_line_idx:])
    match = re.match(
      r'^\s*"""(.*?)"""\s*',
      text,
      re.DOTALL | re.MULTILINE
    ) 

    if not match:
        return "", current_line_idx

    description = match.group(1).strip()
    matched_text = match.group(0)
    lines_consumed = matched_text.count('\n') + 1
    
    description = ' '.join(description.split())
    
    return description, current_line_idx + lines_consumed

def escape_identifier(name: str) -> str:
    return f'"{name}"'

def escape_description(desc: Optional[str]) -> str:
    """Экранирует описание для вставки в SQL"""
    if not desc:
        return 'NULL'
    
    # Заменяем кавычки на экранированные
    escaped = desc.replace("'", "''")
    # Удаляем переносы строк и лишние пробелы
    escaped = ' '.join(escaped.split())
    return f"'{escaped}'"

def parse_operation_types(schema_content: str, type_suffix: str) -> Dict[str, List[Dict]]:
    """Парсит типы с указанным суффиксом (Queries/Mutations) с поддержкой многострочных описаний"""
    operations = defaultdict(list)
    current_type = None
    current_description = []
    in_multiline_description = False
    
    lines = schema_content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        current_description 
        # Обработка типа операции
        if line.startswith("type ") and line.endswith(f"{type_suffix} {{"):
            current_type = re.match(r"type (\w+)", line).group(1)
            current_description = []
        elif line == "}" and current_type:
            current_type = None
        elif current_type and ":" in line:
            # Проверяем наличие однострочного описания
            single_line_desc = re.search(r'"""(.*?)"""', line)
            if single_line_desc:
                current_description = [single_line_desc.group(1)]


                operations[current_type].append(parse_op_line(line))
                current_description = []
        elif line.startswith('"""') and line.endswith('"""'):
            # Однострочное описание перед операцией
            current_description = [line[3:-3].strip()]
        
        i += 1
    
    return operations

def generate_operations_tables(operations_data: Dict, table_suffix: str) -> Tuple[str, str]:
    """Генерирует SQL для операционных таблиц"""
    tables_ddl = []
    inserts_ddl = []
    
    for op_type, operations in operations_data.items():
        table_name = op_type
        
        create_table = f"""
CREATE TABLE IF NOT EXISTS {escape_identifier(table_name)} (
  "operationName" TEXT PRIMARY KEY,
  "description" TEXT,
  "params" JSONB,
  "returns" TEXT
);
"""
        tables_ddl.append(create_table)
        
        for op in operations:
            insert = f"""
INSERT INTO {escape_identifier(table_name)} 
  ("operationName", "description", "params", "returns") 
VALUES 
  ('{op["operationName"]}', 
   {escape_description(op.get('description'))}, 
   '{json.dumps(op["params"])}', 
   '{op["returns"]}')
ON CONFLICT ("operationName") DO NOTHING;
"""
            inserts_ddl.append(insert)
    
    return "\n".join(tables_ddl), "\n".join(inserts_ddl)

def parse_graphql_schema(schema_content: str) -> Dict:
    types = {}
    enums = {}
    relations = []
    current_type = None
    nested_types = defaultdict(dict)  # Для хранения вложенных типов

    for line in schema_content.split('\n'):
        line = line.strip()
        
        if (
            line.startswith("type ") 
            and "{" in line
            and not line.endswith("Queries {")
            and not line.endswith("Mutations {")
        ):
            type_name = re.match(r"type (\w+)", line).group(1)
            types[type_name] = {"fields": [], "pk": None, "nested": {}}
            current_type = ("type", type_name)
        
        elif line.startswith("enum ") and "{" in line:
            enum_name = re.match(r"enum (\w+)", line).group(1)
            enums[enum_name] = []
            current_type = ("enum", enum_name)
        
        elif line == "}":
            current_type = None
        
        elif current_type:
            if current_type[0] == "type":
                # Обработка вложенных типов
                if ":" not in line and "{" in line:
                    nested_type = line.split("{")[0].strip()
                    nested_types[current_type[1]][nested_type] = []
                elif ":" in line:
                    field_match = re.match(r"(\w+):\s*(!?\[?[\w\!\]\s]+)", line)
                    if field_match:
                        field_name, field_type = field_match.groups()
                        is_array = '[' in field_type
                        clean_type = field_type.replace('!', '').replace('[', '').replace(']', '').strip()
                        
                        # if field_name == "id" or clean_type == "ID":
                        #     types[current_type[1]]["pk"] = (field_name, clean_type)
                        
                        if clean_type in nested_types.get(current_type[1], {}):
                            types[current_type[1]]["nested"][field_name] = clean_type
                        elif clean_type in enums:
                            types[current_type[1]]["fields"].append((field_name, clean_type, is_array))
                        elif clean_type in types and clean_type != current_type[1]:
                            relations.append((current_type[1], field_name, clean_type, is_array))
                        else:
                            types[current_type[1]]["fields"].append((field_name, clean_type, is_array))
            
            elif current_type[0] == "enum" and line not in ("{", "}"):
                enums[current_type[1]].append(line.strip())

        # Парсим отдельно Queries и Mutations
    queries = parse_operation_types(schema_content, "Queries")
    mutations = parse_operation_types(schema_content, "Mutations")
    return {
        "types": types,
        "enums": enums, 
        "relations": relations,
        "queries": queries,
        "mutations": mutations
    }

def generate_ddl(schema_data: Dict) -> Tuple[str, str]:
    tables_ddl = ["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n"]
    fk_ddl = []
    created_enums = set()
    junction_tables = set()

    # Создаем ENUM-типы
    for enum_name, enum_values in schema_data["enums"].items():
        escaped_name = escape_identifier(enum_name)
        values = ', '.join([f"'{v}'" for v in enum_values if v.strip()])
        tables_ddl.append(f"CREATE TYPE {escaped_name} AS ENUM ({values});")
        created_enums.add(enum_name)

    # Создаем таблицы
    for table, data in schema_data["types"].items():
        columns = []
        
        # Первичный ключ (если есть)
        if data["pk"]:
            pk_name, pk_type = data["pk"]
            if pk_type == "UUID":
                pk_def = "UUID PRIMARY KEY DEFAULT uuid_generate_v4()"
            else:  # ID
                pk_def = "SERIAL PRIMARY KEY"
            
            columns.append(f"{escape_identifier(pk_name)} {pk_def}")

        # Обработка вложенных типов
        for nested_field, nested_type in data.get("nested", {}).items():
            columns.append(f"{escape_identifier(nested_field)} JSONB")

        # Обычные поля
        type_mapping = {
            "ID": "INTEGER",
            "UUID": "UUID",
            "String": "TEXT",
            "Int": "INTEGER",
            "Boolean": "BOOLEAN",
            "DateTime": "DATE",
            "Float": "FLOAT",
        }

        for field_info in data["fields"]:
            if len(field_info) == 3:
                field_name, field_type, is_array = field_info
            else:
                field_name, field_type = field_info
                is_array = False
            
            # Пропускаем поля-массивы таблиц (будем обрабатывать отдельно)
            if is_array and field_type in schema_data["types"]:
                continue
                
            if field_type in created_enums:
                sql_type = escape_identifier(field_type)
            elif field_type in type_mapping:
                sql_type = type_mapping[field_type]
            else:
                sql_type = "TEXT"
            
            if is_array:
                sql_type = f"{sql_type}[]"
            
            columns.append(f"{escape_identifier(field_name)} {sql_type}")

        if columns:
            columns_str = ',\n  '.join(columns)
            tables_ddl.append(
                f"CREATE TABLE {escape_identifier(table)} (\n"
                f"  {columns_str}\n"
                ");"
            )

        if "nested" in data:
            for nested_field, nested_type in data["nested"].items():
                # Для вложенных типов используем JSONB
                columns.append(f"{escape_identifier(nested_field)} JSONB")

    # Обработка связей
    for relation in schema_data["relations"]:
        if len(relation) == 4:
            src_table, fk_col, target_table, is_array = relation
        else:
            src_table, fk_col, target_table = relation
            is_array = False
        
        # Получаем первый столбец целевой таблицы, если нет PK
        target_fields = schema_data["types"][target_table]["fields"]
        if not target_fields:
            continue  # Пропускаем таблицы без полей
            
        target_col = target_fields[0][0]
        target_col_type = target_fields[0][1]
        
        # Определяем тип столбца
        if target_col_type in created_enums:
            fk_type = escape_identifier(target_col_type)
        elif target_col_type == "UUID":
            fk_type = "UUID"
        else:
            fk_type = "INTEGER"
        
        if is_array:
            # Создаем junction-таблицу для many-to-many
            junction_table = f"{src_table}_{target_table}"
            if junction_table not in junction_tables:
                fk_ddl.append(f"""
CREATE TABLE {escape_identifier(junction_table)} (
  {escape_identifier(src_table)} {fk_type} REFERENCES {escape_identifier(src_table)}({escape_identifier(target_col)}),
  {escape_identifier(target_table)} {fk_type} REFERENCES {escape_identifier(target_table)}({escape_identifier(target_col)}),
  PRIMARY KEY ({escape_identifier(src_table)}, {escape_identifier(target_table)})
);
""")
                junction_tables.add(junction_table)
        else:
            # Обычная 1-to-many связь
            fk_ddl.append(f"""
ALTER TABLE {escape_identifier(src_table)}
  ADD COLUMN {escape_identifier(fk_col)} {fk_type},
  ADD FOREIGN KEY ({escape_identifier(fk_col)})
  REFERENCES {escape_identifier(target_table)}({escape_identifier(target_col)});
""")

    # Генерация таблиц для запросов и мутаций
    queries_tables, queries_inserts = generate_operations_tables(
        schema_data["queries"], "queries")
    mutations_tables, mutations_inserts = generate_operations_tables(
        schema_data["mutations"], "mutations")
    
    tables_ddl.extend([queries_tables, mutations_tables])
    fk_ddl.extend([queries_inserts, mutations_inserts])


    return "\n".join(tables_ddl), "\n".join(fk_ddl)

if __name__ == "__main__":
    with open("schema.txt", "r") as f:
        schema = parse_graphql_schema(f.read())
    
    tables_sql, fk_sql = generate_ddl(schema)
    
    with open("01_tables.sql", "w") as f:
        f.write(tables_sql)
    
    with open("02_relations.sql", "w") as f:
        f.write(fk_sql)