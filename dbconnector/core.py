import pandas as pd
from sqlalchemy import create_engine

from dbconnector.conf import prepare_cfg
from dbconnector.write import TABLE_WRITE, WRITE_MODES
from dbconnector.vars import DB_SCHEMA, SS_CURSORS


# декоратор для проверки наличия базы в конфиге
def check_db(f):
    def wrapper(db, *args, **kwargs):
        CFG = prepare_cfg()
        if CFG.get("DB") is None:
            raise Exception(f"Not found 'DB' key in config")
        elif db in CFG["DB"]:
            return f(db, *args, **kwargs, CFG = CFG)
        else:
            raise Exception(f"DB '{db}' not found in config")

    return wrapper


# получить uri строку для коннекта
@check_db
def get_db_string( db, CFG = {} ):
    schema = DB_SCHEMA.get( CFG["DB"][ db ]["dbtype"] )
    user = CFG["DB"][ db ]["user"]
    password = CFG["DB"][ db ]["password"]
    host = CFG["DB"][ db ]["host"]
    port = CFG["DB"][ db ]["port"]
    db = CFG["DB"][ db ]["db"]

    return f"{schema}://{user}:{password}@{host}:{port}/{db}"


# получить коннект
@check_db
def get_db_conn( db, debug = False, CFG = {} ):
    return create_engine( get_db_string(db), encoding = "utf8", echo = debug )


# получить курсор
@check_db
def get_db_cursor( db, server_side = False, debug = False, CFG = {} ):
    db_cfg = CFG["DB"][ db ]

    cursor = {}
    if server_side and db_cfg["dbtype"] in SS_CURSORS:
        cursor = SS_CURSORS.get( db_cfg["dbtype"] )

    return get_db_conn(db, debug).raw_connection().cursor(**cursor)


# прочитать датафрейм
@check_db
def read_sql( db, sql, params = None, debug = False, CFG = {}, **kwargs ):
    if CFG["DB"][ db ]["dbtype"] == "clickhouse":
        sql = f"{sql}\nFORMAT TabSeparatedWithNamesAndTypes"

    return pd.read_sql( sql, con = get_db_conn(db, debug), params = params, **kwargs )


# записать датафрейм
# write_mode ("new", "append", "recreate")
# new - создать новую таблицу
# recreate - пересоздать таблицу
# append - дописать данные
@check_db
def write_sql(
    db, table, df, index_col = None, write_mode = "new",
    dbname = "", debug = False, CFG = {}
):
    # если не указана индексная колонка, создаем
    if index_col is None:
        df = df.reset_index()
        index_col = "index"

    # если указана БД, меняем
    db_cfg = CFG["DB"][ db ]
    if dbname:
        db_cfg["db"] = dbname

    # создаем таблицу
    if write_mode in WRITE_MODES:
        args = (db, db_cfg, table, df, index_col, get_db_cursor, read_sql)
        table = WRITE_MODES.get(write_mode)(*args)
    else:
        return False

    # пишем данные
    if db_cfg["dbtype"] in TABLE_WRITE:

        # на всякий случай проверяем колонки в df и БД
        inp_cols = df.columns.tolist()
        out_cols = read_sql(db ,f"SELECT * FROM {table} LIMIT 0").columns.tolist()
        df = df[ list( set(inp_cols) & set(out_cols) ) ]

        write_table = TABLE_WRITE.get( db_cfg["dbtype"] )[1]
        return write_table( df, table, get_db_conn(db, debug) )
    else:
        return False


# получить словарь из конфига
def get_config(key):
    return prepare_cfg().get(key)
