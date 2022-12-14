import pymysql

# server side cursors
SS_CURSORS = {
    "mysql" : { "cursor" : pymysql.cursors.SSCursor },
    "postgres" : { "name" : "pg_ssc" },
    "clickhouse" : {},
}

DEFAULT_VALUES = {
    "mysql" : {
        "port" : 3306
    },
    "postgres" : {
        "port" : 5432
    },
    "clickhouse" : {
        "port" : 8123,
        "cluster" : False
    }
}

DB_SCHEMA = {
    "mysql" : "mysql+pymysql",
    "postgres" : "postgresql+psycopg2",
    "clickhouse" : "clickhouse"
}

COLUMN_TEMPLATE = "  `{col_name}` {col_type}"

### MYSQL
MY_TYPES_MAPPING = {
    "bool" : "BOOL",
    "uint8" : "INT",
    "int32" : "INT",
    "int64" : "INT",
    "float64" : "DECIMAL(15,2)",
    "datetime64[ns]" : "DATETIME",
    "object" : "TEXT"
}

MY_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {dbname}{table} (
{columns}{PK}
)
"""

### POSTGRES


### CLICKHOUSE
CH_TYPES_MAPPING = {
    "bool" : "UInt8",
    "uint8" : "UInt8",
    "int32" : "Int32",
    "int64" : "Int64",
    "float64" : "Float64",
    "datetime64[ns]" : "Nullable(DateTime)",
    "object" : "String"
}

# шаблон для локальной таблицы
CH_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {dbname}.{table} (
{columns},
  `event_date` MATERIALIZED toDate({datetime_column})
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY {index_column}
SETTINGS index_granularity = 8192
"""

# шаблон для distributed таблицы
CH_TABLE_DIST_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {dbname}.{table} ON CLUSTER '{cluster}' (
{columns},
  `event_date` Date DEFAULT toDate({datetime_column})
)
ENGINE = Distributed( '{cluster}', {dbname}, {table}_replicated, rand() )
"""

# шаблон для replicated таблицы
CH_TABLE_REPL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {dbname}.{table}_replicated ON CLUSTER '{cluster}' (
{columns},
  `event_date` Date DEFAULT toDate({datetime_column})
)
ENGINE = ReplicatedMergeTree('{path}/{shard}/{dbname}/{table}_replicated', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY {index_column}
SETTINGS index_granularity = 8192
"""
