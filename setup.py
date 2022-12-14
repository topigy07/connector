from setuptools import setup

setup(
    name = "dbconnector",
    version = "0.1.5",
    packages = ["dbconnector"],
    description = "DS dbconnector",
    license = "MIT",
    author = "Vladislav Filippov",
    author_email = "philippov@reg.ru",
    keywords = ["dbconnector"],
    url = "https://github.com/topigy07/connector",
    install_requires = [
        "python-dotenv", "pyyaml", "pandas", "numpy",
        "pymysql", "psycopg2-binary", "clickhouse-sqlalchemy==0.1.7"
    ],
)
