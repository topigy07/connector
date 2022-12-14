from dbconnector.mysql import my_create_table
# from dbconnector.postgres import pg_create_table
from dbconnector.clickhouse import ch_create_table


# пишем df в указанную таблицу
def write_df(df, table, con):
    try:
        df.to_sql(name = table, index = False, con = con, if_exists = "append")
        return True
    except:
        return False


# create and write dict
TABLE_WRITE = {
    "mysql" : (my_create_table, write_df),
    # "postgres" : (pg_create_table, write_df),
    "clickhouse" : (ch_create_table, write_df)
}


# пересоздаем таблицу в БД
def recreate_table(db, db_cfg, table, df, index_col, get_db_cursor, read_sql):
    kwargs = { "cluster" : db_cfg.get("cluster") }
    template = "DROP TABLE IF EXISTS {table}{query}"
    sqls = [ template.format( table = table, query = "" ) ]
    # для кластера нужно удалять distributed и replicated таблицы
    if kwargs.get("cluster"):
        query = " ON CLUSTER '{cluster}'"
        sqls = [
            template.format( table = table, query = query ),
            template.format( table = f"{table}_replicated", query = query )
        ]

    create_table = TABLE_WRITE.get( db_cfg["dbtype"] )[0]
    args = (df, db_cfg["db"], table, index_col)

    # выполняем запросы на удаление и создание таблиц
    cur = get_db_cursor(db)
    for sql in sqls + create_table(*args, **kwargs):
        cur.execute(sql)

    return table


# создаем новую таблицу в БД. Если существует добавляем порядковый номер
def create_new_table(db, db_cfg, table, df, index_col, get_db_cursor, read_sql):
    is_exists, idx = True, 0

    # ищем номер для новой таблицы
    while is_exists:
        new_table = table if idx == 0 else f"{table}_{idx}"
        name = "name" if db_cfg["dbtype"] == "clickhouse" else f"Tables_in_{ db_cfg['db'] }"
        sql = f"SHOW TABLES WHERE {name} = '{new_table}'"
        is_exists = not read_sql(db, sql).empty
        idx += 1

    create_table = TABLE_WRITE.get( db_cfg["dbtype"] )[0]
    args = (df, db_cfg["db"], new_table, index_col)
    kwargs = { "cluster" : db_cfg.get("cluster") }

    # создаем новую таблицу
    cur = get_db_cursor(db)
    for sql in create_table(*args, **kwargs):
        cur.execute(sql)

    return new_table


# если таблицы не существует, создаем ее
def create_table_if_not_exists(db, db_cfg, table, df, index_col, get_db_cursor, read_sql):
    name = "name" if db_cfg["dbtype"] == "clickhouse" else f"Tables_in_{ db_cfg['db'] }"
    sql = f"SHOW TABLES WHERE {name} = '{table}'"

    create_table = TABLE_WRITE.get( db_cfg["dbtype"] )[0]
    args = (df, db_cfg["db"], table, index_col)
    kwargs = { "cluster" : db_cfg.get("cluster") }

    if read_sql(db, sql).empty:
        cur = get_db_cursor(db)
        for sql in create_table(*args, **kwargs):
            cur.execute(sql)

    return table


WRITE_MODES = {
    "new" : create_new_table,
    "recreate" : recreate_table,
    "append" : create_table_if_not_exists,
}
