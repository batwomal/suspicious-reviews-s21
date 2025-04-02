from graphql import build_schema, get_named_type
from graphql.type import (
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLList,
    GraphQLField,
    GraphQLType,
    GraphQLEnumType
)
from typing import Dict, Set, List, Tuple, Optional
import json

class Convertor:

    class CustomSchema(GraphQLSchema):
        enum_map: Dict[str, GraphQLEnumType]
        inputs_map: Dict[str, GraphQLInputObjectType]
        queriy_map: Dict[str, GraphQLObjectType]
        mutation_map: Dict[str, GraphQLObjectType]


    schema: CustomSchema = None
    def __init__(self, schema_pth: str):

        try:
            with open(schema_pth, "r") as f:
                self.schema = build_schema(f.read())
        except Exception as e:
            print(f'Error building schema: {e}')


    def count_schema_types(self) -> Dict[str, int]:
        """
        Считает количество различных типов в GraphQL-схеме.
        
        Возвращает словарь с количеством:
        {
            'types': int,         # Все обычные типы (type)
            'inputs': int,        # Input-типы
            'enums': int,         # Перечисления
            'interfaces': int,    # Интерфейсы
            'unions': int,        # Юнионы
            'scalars': int,       # Скалярные типы
            'total': int          # Общее количество типов
        }
        """
        counts = {
            'types': 0,
            'queries': 0,
            'mutations': 0,
            'subscriptions': 0,
            'inputs': 0,
            'enums': 0,
            'interfaces': 0,
            'unions': 0,
            'scalars': 0,
            'total': 0
        }
        queries = self.get_root_operation_types(self.schema, 'query')
        mutations = self.get_root_operation_types(self.schema, 'mutation')
        subscriptions = self.get_root_operation_types(self.schema, 'subscription')
        
        for type_name, type_obj in self.schema.type_map.items():
            # Пропускаем служебные типы (__Schema, __Type и т.д.)
            if type_name.startswith('__'):
                continue
                
            counts['total'] += 1
            
            if isinstance(type_obj, GraphQLObjectType):
                # Пропускаем Root-типы (Query, Mutation, Subscription)
                if (
                    type_name not in ('Query', 'Mutation', 'Subscription')
                    # and type_name not in queries
                    # and type_name not in mutations
                    # and type_name not in subscriptions
                ):
                    counts['types'] += 1
            elif isinstance(type_obj, GraphQLInputObjectType):
                counts['inputs'] += 1
            elif isinstance(type_obj, GraphQLEnumType):
                counts['enums'] += 1
            elif isinstance(type_obj, GraphQLInterfaceType):
                counts['interfaces'] += 1
            elif isinstance(type_obj, GraphQLUnionType):
                counts['unions'] += 1
            elif isinstance(type_obj, GraphQLScalarType):
                # Пропускаем встроенные скаляры (String, Int и т.д.)
                if type_name not in ['String', 'Int', 'Float', 'Boolean', 'ID']:
                    counts['scalars'] += 1

        counts['queries'] = len(queries)
        counts['mutations'] = len(mutations)
        counts['subscriptions'] = len(subscriptions)

        return counts

    def get_root_operation_types(self, schema: GraphQLSchema, operation_type: str) -> List[str]:
        """
        Возвращает типы, используемые в указанном корневом типе.
        
        Параметры:
            schema: GraphQL-схема
            operation_type: 'query', 'mutation' или 'subscription'
        
        Возвращает:
            Множество имен используемых типов
        """

        # Динамически получаем корневой тип
        root_type = getattr(schema, f"{operation_type}_type", None)
        
        if not root_type:
            return list()
        
        types = list()
        for field in root_type.fields.values():
            # Обрабатываем возвращаемый тип
            return_type = get_named_type(field.type)
            types.append(return_type.name)
            
            # Обрабатываем аргументы
            for arg in field.args.values():
                arg_type = get_named_type(arg.type)
                types.append(arg_type.name)

        return types
        
    def get_operation_definitions(self, schema: GraphQLSchema, operation_types: List[str]) -> List[Dict]:
        """
        Returns a list of operation definitions in the given GraphQL schema.
        
        Parameters:
            schema: GraphQLSchema
            operation_types: List of GraphQL operation types (e.g. query, mutation, subscription)
        
        Returns:
            List of operation definitions, where each definition is a dictionary with the following keys:
                - type: The name of the operation type (e.g. Query, Mutation, Subscription)
                - description: The description of the operation type
                - fields: A list of dictionaries, where each dictionary contains information about a field of the operation type
                    - name: The name of the field
                    - description: The description of the field
                    - type: A dictionary with information about the type of the field
                        - name: The name of the type
                        - is_required: Whether the type is required
                        - is_list: Whether the type is a list
                        - is_list_item_required: Whether the items in the list are required
                        - of_type: The type of the items in the list
                    - args: A dictionary with information about the arguments of the field
                        - arg_name: The name of the argument
                        - type: A dictionary with information about the type of the argument
                            - name: The name of the type
                            - is_required: Whether the type is required
                            - is_list: Whether the type is a list
                            - is_list_item_required: Whether the items in the list are required
                            - of_type: The type of the items in the list
        """

        def _get_type_info(type_obj) -> Dict[str, any]:
            """Возвращает информацию о типе с модификаторами"""
            type_info = {
                'name': None,
                'is_required': False,
                'is_list': False,
                'is_list_item_required': False,
                'of_type': None
            }
            
            # Обрабатываем NonNull (обязательные типы)
            if type_obj.__class__.__name__ == 'GraphQLNonNull':
                type_info['is_required'] = True
                type_obj = type_obj.of_type
            
            # Обрабатываем List (массивы)
            if type_obj.__class__.__name__ == 'GraphQLList':
                type_info['is_list'] = True
                # Получаем информацию о типе элементов массива
                type_info['of_type'] = _get_type_info(type_obj.of_type)
                # Определяем обязательность элементов массива
                type_info['is_list_item_required'] = type_info['of_type']['is_required']
                return type_info
            
            # Для именованных типов (не List и не NonNull)
            type_info['name'] = type_obj.name
            return type_info

        definitions = list()
        
        for type_name in operation_types:
            type_obj = schema.get_type(type_name)
            if not type_obj:
                continue
            if type_name.startswith('__'):
                continue
            # if type_name in ['Int', 'String', 'Boolean', 'ID', 'Float']:
            #     continue
            if not isinstance(type_obj, GraphQLObjectType):
                continue
            
            operation_def = {
                'type': type_obj.name,
                'description': type_obj.description,
                'fields': list()
            }


            for field_name, field in type_obj.fields.items():
                field_def = {
                    'name': field_name,
                    'description': field.description,
                    'type': _get_type_info(field.type),  # Модифицированная обработка типа
                    'args': {
                        arg_name: _get_type_info(arg.type) 
                        for arg_name, arg in field.args.items()
                    }
                }
                operation_def['fields'].append(field_def)
            
            definitions.append(operation_def)
        
        return definitions

    def get_special_types(self, type_prefix: str) -> Dict[str, GraphQLType]:
        mapping = {
            'enum' : GraphQLEnumType,
            'input' : GraphQLInputObjectType,
            'interface' : GraphQLInterfaceType,
            'type' : GraphQLObjectType,
            'scalar' : GraphQLScalarType,
            'union' : GraphQLUnionType,
        }
        return {k: v for k, v in self.schema.type_map.items() if isinstance(v, mapping[type_prefix])}

    def escape_identifier(self, name: str) -> str:
        return f'"{name}"'

    def escape_description(self, desc: Optional[str]) -> str:
        """Экранирует описание для вставки в SQL"""
        if not desc:
            return 'NULL'
        
        # Заменяем кавычки на экранированные
        escaped = desc.replace("'", "''")
        # Удаляем переносы строк и лишние пробелы
        escaped = ' '.join(escaped.split())
        return f"'{escaped}'"

    def create_enums_ddl(self, schema: GraphQLSchema) -> str:
        enums_ddl = []
        for enum_name, enum_values in get_special_types(schema, 'enum').items():
            escaped_name = escape_identifier(enum_name)
            values = ', '.join(
                [
                    f"'{v}'" 
                    for v 
                    in enum_values.values.keys()
                    if v.strip()
                ]
            )
            del schema.type_map[enum_name]
            enums_ddl.append(f"CREATE TYPE {escaped_name} AS ENUM ({values});")
        return '\n'.join(enums_ddl)

    def extract_columns(self, schema: GraphQLSchema, table_name: str) -> List[str]:
        columns_data = schema.get_type(table_name).fields
        columns = { }
        type_mapping = {
            "ID": "INTEGER",
            "UUID": "UUID",
            "String": "TEXT",
            "Int": "INTEGER",
            "Boolean": "BOOLEAN",
            "DateTime": "DATETIME",
            "Date": "DATE",
            "Float": "FLOAT",
        }

    def create_table_ddl(self, schema: GraphQLSchema, table_name: str) -> str:
        table_ddl = []
        columns = schema.get_type(table_name).fields
    #         if columns:
    #             columns_str = ',\n  '.join(columns)
    #             tables_ddl.append(
    #                 f"CREATE TABLE {escape_identifier(table)} (\n"
    #                 f"  {columns_str}\n"
    #                 ");"
    #             )
        return '\n'.join(table_ddl)

    def create_operation_tables_ddl(self, schema: GraphQLSchema, operation_type: str) -> str:
        operation_tables_ddl = []
        operations = schema.get_type(operation_type).fields
        
        return '\n'.join(operation_tables_ddl)

    def generate_ddl(self, schema: GraphQLSchema) -> Tuple[str, str]:
        tables_ddl = ["CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n"]
        fk_ddl = []
        junction_tables = set()

        tables_ddl.append(create_enums_ddl(schema))
        tables_ddl.append(create_operation_tables_ddl(schema, 'Query'))
        tables_ddl.append(create_operation_tables_ddl(schema, 'Mutation'))
        tables_ddl.append(create_operation_tables_ddl(schema, 'Subscription'))
    
    
    #     # Создаем таблицы
    #     for table, data in schema_data["types"].items():
    #         columns = []
            
    #         # Первичный ключ (если есть)
    #         if data["pk"]:
    #             pk_name, pk_type = data["pk"]
    #             if pk_type == "UUID":
    #                 pk_def = "UUID PRIMARY KEY DEFAULT uuid_generate_v4()"
    #             else:  # ID
    #                 pk_def = "SERIAL PRIMARY KEY"
                
    #             columns.append(f"{escape_identifier(pk_name)} {pk_def}")

    #         # Обработка вложенных типов
    #         for nested_field, nested_type in data.get("nested", {}).items():
    #             columns.append(f"{escape_identifier(nested_field)} JSONB")

    #         # Обычные поля
    #         type_mapping = {
    #             "ID": "INTEGER",
    #             "UUID": "UUID",
    #             "String": "TEXT",
    #             "Int": "INTEGER",
    #             "Boolean": "BOOLEAN",
    #             "DateTime": "DATE",
    #             "Float": "FLOAT",
    #         }

    #         for field_info in data["fields"]:
    #             if len(field_info) == 3:
    #                 field_name, field_type, is_array = field_info
    #             else:
    #                 field_name, field_type = field_info
    #                 is_array = False
                
    #             # Пропускаем поля-массивы таблиц (будем обрабатывать отдельно)
    #             if is_array and field_type in schema_data["types"]:
    #                 continue
                    
    #             if field_type in created_enums:
    #                 sql_type = escape_identifier(field_type)
    #             elif field_type in type_mapping:
    #                 sql_type = type_mapping[field_type]
    #             else:
    #                 sql_type = "TEXT"
                
    #             if is_array:
    #                 sql_type = f"{sql_type}[]"
                
    #             columns.append(f"{escape_identifier(field_name)} {sql_type}")

    #         if columns:
    #             columns_str = ',\n  '.join(columns)
    #             tables_ddl.append(
    #                 f"CREATE TABLE {escape_identifier(table)} (\n"
    #                 f"  {columns_str}\n"
    #                 ");"
    #             )

    #         if "nested" in data:
    #             for nested_field, nested_type in data["nested"].items():
    #                 # Для вложенных типов используем JSONB
    #                 columns.append(f"{escape_identifier(nested_field)} JSONB")

    #     # Обработка связей
    #     for relation in schema_data["relations"]:
    #         if len(relation) == 4:
    #             src_table, fk_col, target_table, is_array = relation
    #         else:
    #             src_table, fk_col, target_table = relation
    #             is_array = False
            
    #         # Получаем первый столбец целевой таблицы, если нет PK
    #         target_fields = schema_data["types"][target_table]["fields"]
    #         if not target_fields:
    #             continue  # Пропускаем таблицы без полей
                
    #         target_col = target_fields[0][0]
    #         target_col_type = target_fields[0][1]
            
    #         # Определяем тип столбца
    #         if target_col_type in created_enums:
    #             fk_type = escape_identifier(target_col_type)
    #         elif target_col_type == "UUID":
    #             fk_type = "UUID"
    #         else:
    #             fk_type = "INTEGER"
            
    #         if is_array:
    #             # Создаем junction-таблицу для many-to-many
    #             junction_table = f"{src_table}_{target_table}"
    #             if junction_table not in junction_tables:
    #                 fk_ddl.append(f"""
    # CREATE TABLE {escape_identifier(junction_table)} (
    #   {escape_identifier(src_table)} {fk_type} REFERENCES {escape_identifier(src_table)}({escape_identifier(target_col)}),
    #   {escape_identifier(target_table)} {fk_type} REFERENCES {escape_identifier(target_table)}({escape_identifier(target_col)}),
    #   PRIMARY KEY ({escape_identifier(src_table)}, {escape_identifier(target_table)})
    # );
    # """)
    #                 junction_tables.add(junction_table)
    #         else:
    #             # Обычная 1-to-many связь
    #             fk_ddl.append(f"""
    # ALTER TABLE {escape_identifier(src_table)}
    #   ADD COLUMN {escape_identifier(fk_col)} {fk_type},
    #   ADD FOREIGN KEY ({escape_identifier(fk_col)})
    #   REFERENCES {escape_identifier(target_table)}({escape_identifier(target_col)});
    # """)

    #     # Генерация таблиц для запросов и мутаций
    #     queries_tables, queries_inserts = generate_operations_tables(
    #         schema_data["queries"], "queries")
    #     mutations_tables, mutations_inserts = generate_operations_tables(
    #         schema_data["mutations"], "mutations")
        
    #     tables_ddl.extend([queries_tables, mutations_tables])
    #     fk_ddl.extend([queries_inserts, mutations_inserts])


        return "\n".join(tables_ddl), "\n".join(fk_ddl)

if __name__ == "__main__":
    convertor = Convertor('s21schema/schema/schema.gql')

    # print(convertor.count_schema_types())

    print(type(convertor.schema.type_map))

    # print(count_schema_types(gql_schema))

    # with open("test.sql", "w") as f:
    #     f.write(generate_ddl(gql_schema)[0])

    # print(count_schema_types(gql_schema))

    # print(gql_schema.get_type('Query').fields['userChangeRequest'].type.fields['getUserHasChangeRequest'].description)