from dbconnector.vars import (
    CH_TYPES_MAPPING, COLUMN_TEMPLATE, CH_TABLE_TEMPLATE,
    CH_TABLE_DIST_TEMPLATE, CH_TABLE_REPL_TEMPLATE
)


# получить запрос для создания таблицы в CH
def ch_create_table(df, dbname, table, index_col, **kwargs):
    cols = []
    for col in df.dtypes.items():
        col_definition = {
            "col_name" : col[0],
            "col_type" : CH_TYPES_MAPPING.get( str(col[1]) )
        }
        cols.append( COLUMN_TEMPLATE.format(**col_definition) )

    table_definition = {
        "dbname" : dbname,
        "table" : table,
        "datetime_column" : "NOW()",
        "index_column" : index_col,
        "columns" : ",\n".join(cols)
    }
    sqls = [ CH_TABLE_TEMPLATE.format(**table_definition) ]

    # для кластера возвращаем запросы для distributed и replicated таблиц
    if kwargs.get("cluster"):
        for key in ("cluster", "path", "shard", "replica"):
            table_definition[ key ] = "{" + key + "}"
        sqls = [
            CH_TABLE_DIST_TEMPLATE.format(**table_definition),
            CH_TABLE_REPL_TEMPLATE.format(**table_definition)
        ]

    return sqls
