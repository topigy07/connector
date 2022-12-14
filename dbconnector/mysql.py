from dbconnector.vars import (
    MY_TYPES_MAPPING, MY_TABLE_TEMPLATE, COLUMN_TEMPLATE
)


# запрос для создания таблицы в MYSQL
def my_create_table(df, dbname, table, index_col, **kwargs):
    cols = []
    for col in df.dtypes.items():
        col_definition = {
            "col_name" : col[0],
            "col_type" : MY_TYPES_MAPPING.get( str(col[1]) )
        }
        cols.append( COLUMN_TEMPLATE.format(**col_definition) )

    table_definition = {
        "dbname" : f"{dbname}." if dbname != "" else "",
        "table" : table,
        "PK" : f",\n  PRIMARY KEY (`{index_col}`)" if index_col else "",
        "columns" : ",\n".join(cols)
    }

    return [ MY_TABLE_TEMPLATE.format(**table_definition) ]
