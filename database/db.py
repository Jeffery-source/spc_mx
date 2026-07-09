from sqlalchemy import create_engine
from urllib.parse import quote_plus

from config import load_config


config = load_config()


db = config["database"]


connection_url = (
    "mysql+pymysql://"
    f"{db['user']}:"
    f"{quote_plus(db['password'])}@"
    f"{db['host']}:"
    f"{db['port']}/"
    f"{db['name']}"
)


engine = create_engine(
    connection_url,
    pool_pre_ping=True
)