import os
from src.typeDefs.jsonConf import IJsonConf
import json

def getConf(confKey):
    db_name = os.getenv('SCADA_WAREHOUSE_DB_NAME', 'db_name')
    db_username = os.getenv('METER_WAREHOUSE_DB_USERNAME', 'username')
    db_password = os.getenv('METER_WAREHOUSE_DB_PASSWORD', 'password')
    db_host = os.getenv('METER_WAREHOUSE_DB_HOST', 'hostip')
    db_port = os.getenv('METER_WAREHOUSE_DB_PORT', 'db_port')
    if confKey == "dbConfig":
        return dict(
            db_name=db_name,
            db_username=db_username,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port
        )


def getJsonConfig(fPath="config.json") -> IJsonConf:
    with open(fPath) as f:
        jsonConf = json.load(f)
    return jsonConf
