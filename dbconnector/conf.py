import os
import yaml

from dbconnector.vars import DEFAULT_VALUES


# читаем конфиг
def load_config(path):
    if not os.path.exists(path):
        return {}

    return yaml.load(
        stream = open( path, encoding = "utf8" ),
        Loader = yaml.SafeLoader
    )


# устанавливаем дефолтные значения
def set_default_values(db_conf):

    # тип базы
    if "dbtype" not in db_conf.keys():
        db_conf["dbtype"] = "mysql"

    # порт
    if "port" not in db_conf.keys():
        db_conf["port"] = DEFAULT_VALUES[ db_conf["dbtype"] ].get("port")

    # БД
    if "db" not in db_conf.keys():
        db_conf["db"] = ""

    # clickhouse cluster flag
    if db_conf["dbtype"] == "clickhouse" and "cluster" not in db_conf.keys():
        db_conf["cluster"] = DEFAULT_VALUES[ db_conf["dbtype"] ].get("cluster")

    return db_conf


# читаем конфиг и заполняем дефолтные значения
def prepare_cfg():
    cfg = {}
    if os.environ.get("CONFIG_PATH"):
        cfg = load_config( os.environ["CONFIG_PATH"] )

        db_list = []
        if cfg.get("DB"):
            if len( cfg["DB"] ) > 0:
                for db, db_conf in cfg["DB"].items():
                    db_list.append(db)
                    cfg["DB"][ db ] = set_default_values(db_conf)

    return cfg
