import pandas as pd
import requests
import os
import json
import time

# connector_name = list(os.environ['CONNECTOR_NAME'].split(","))
# print(connector_name)

def check_existing(name):
    endpoint = requests.get('http://localhost:8083/connectors')
    response_endpoint = endpoint.json()
    existing_connector = ' '.join(response_endpoint)
    #existing_connector = ['mysql_dev','mysql_staging,']
    new_connector = list(name.split(","))
    #print(new_connector)
    for x in new_connector:
        if x in existing_connector:
            if 'mysql' in x:
                update_mysql_connector(x)
                time.sleep(8)
            elif 'redis' in x:
                update_redis_sink(x)
                time.sleep(8)
            else:
                print('connector type is not defined')
        else:
            if 'mysql' in x:
                create_mysql_connector(x)
                time.sleep(8)
            elif 'redis' in x:
                create_redis_sink(x)
                time.sleep(8)
            else:
                print('connector type is not defined')

def get_user_password(name):
    connector_name = list(os.environ['CONNECTOR_NAME'].split(","))
    connector_host = list(os.environ['CONNECTOR_HOST'].split(","))
    user_name = list(os.environ['DB_USER'].split(","))
    user_password = list(os.environ['DB_USER_PASSWORD'].split(","))
    df = pd.DataFrame(list(zip(connector_name,connector_host,user_name,user_password)),columns=['connector_name','connector_host','user_name','user_password'])
    data = df.query("connector_name == @name")
    host = data['connector_host'].iloc[0]
    user = data['user_name'].iloc[0]
    password = data['user_password'].iloc[0]
    
    #print(redis_db)
    return host,user,password

def get_mysql_db(name):
    connector_name = list(os.environ['CONNECTOR_NAME'].split(","))
    mysql_id = list(os.environ['MYSQL_ID'].split(","))
    topic_prefix = list(os.environ['TOPIC_PREFIX'].split(","))
    signal_table = list(os.environ['SIGNAL_TABLE'].split(","))
    db_tz = list(os.environ['DB_TZ'].split(","))
    db_list = list(os.environ['DB_LIST'].split("-"))
    table_list = list(os.environ['TABLE_LIST'].split("-"))
    df = pd.DataFrame(list(zip(connector_name,mysql_id,topic_prefix,signal_table,db_tz,db_list,table_list)),columns=['connector_name','mysql_id','topic_prefix','signal_table','db_tz','db_list','table_list'])
    data = df.query("connector_name == @name")
    db_id = data['mysql_id'].iloc[0]
    prefix = data['topic_prefix'].iloc[0]
    debezium_signal = data['signal_table'].iloc[0]
    tz = data['db_tz'].iloc[0]
    table = data['table_list'].iloc[0]
    dbs = data['db_list'].iloc[0]

    return db_id,prefix,debezium_signal,tz,table,dbs

def get_redis_db(name):
    connector_name = list(os.environ['CONNECTOR_NAME'].split(","))
    redis = list(os.environ['REDIS_DB'].split(","))
    regex = list(os.environ['TOPIC_PREFIX'].split(","))
    df = pd.DataFrame(list(zip(connector_name,redis,regex)),columns=['connector_name','redis_db','redis_regex'])
    data = df.query("connector_name == @name")
    redis_db = data['redis_db'].iloc[0]
    redis_regex = data['redis_regex'].iloc[0]

    #print(redis_db)
    return redis_db,redis_regex


def create_mysql_connector(name):
    db_host,db_user,db_pass = get_user_password(name)
    db_id,prefix,signal_table,tz,table,dbs = get_mysql_db(name)
    #print(f'MYSQL hostname : {host} username : {user} password : {password}')
    #server_name = name.split("_")[1]

    json_data = {
    "name": f"{name}",
    "config": {
        "connector.class":"io.debezium.connector.mysql.MySqlConnector",
        "database.user":f"{db_user}",
        "database.server.id":f"{db_id}",
        "tasks.max":"1",
        "database.history.kafka.bootstrap.servers":f"{os.environ['KAFKA_BOOT']}",
        "database.history.kafka.topic":f"kafka_{name}",
        "transforms":"unwrap",
        "time.precision.mode":"adaptive_time_microseconds",
        "database.server.name":f"{prefix}",
        "database.port":"3306",
        "include.schema.changes":"false",
        "key.converter.schemas.enable":"false",
        "database.hostname":f"{db_host}",
        "database.password":f"{db_pass}",
        "transforms.unwrap.drop.tombstones":"false",
        "value.converter.schemas.enable":"false",
        "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState",
        "table.include.list":f"{table}",
        "value.converter":"org.apache.kafka.connect.json.JsonConverter",
        "key.converter":"org.apache.kafka.connect.json.JsonConverter",
        "database.include.list":f"{dbs}",
        "decimal.handling.mode": "double",
        "signal.data.collection": f"{signal_table}",
        "database.serverTimezone" : f"{tz}",
        "database.allowPublicKeyRetrieval":"true",
        }
    }
    create_connector = requests.post('http://localhost:8083/connectors', json=json_data)
    print("status code: ", create_connector.status_code)
    print("repose:")
    print(create_connector.json())
    #print(json_data)

def update_mysql_connector(name):
    db_host,db_user,db_pass = get_user_password(name)
    #print(f'MYSQL hostname : {host} username : {user} password : {password}')
    db_id,prefix,signal_table,tz,table,dbs = get_mysql_db(name)

    json_data = {
        "connector.class":"io.debezium.connector.mysql.MySqlConnector",
        "database.user":f"{db_user}",
        "database.server.id": f"{db_id}",
        "tasks.max":"1",
        "database.history.kafka.bootstrap.servers":f"{os.environ['KAFKA_BOOT']}",
        "database.history.kafka.topic":f"kafka_{name}",
        "transforms":"unwrap",
        "time.precision.mode":"adaptive_time_microseconds",
        "database.server.name":f"{prefix}",
        "database.port":"3306",
        "include.schema.changes":"false",
        "key.converter.schemas.enable":"false",
        "database.hostname":f"{db_host}",
        "database.password":f"{db_pass}",
        "transforms.unwrap.drop.tombstones":"false",
        "value.converter.schemas.enable":"false",
        "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState",
        "table.include.list":f"{table}",
        "value.converter":"org.apache.kafka.connect.json.JsonConverter",
        "key.converter":"org.apache.kafka.connect.json.JsonConverter",
        "database.include.list":f"{dbs}",
        "decimal.handling.mode": "double",
        "signal.data.collection": f"{signal_table}",
        "database.serverTimezone" : f"{tz}",
        "database.allowPublicKeyRetrieval":"true",
    }
    update_connector = requests.put(f'http://localhost:8083/connectors/{name}/config', json=json_data)
    print("status code: ", update_connector.status_code)
    print("repose:")
    print(update_connector.json())
    #print(json_data)

def create_redis_sink(name):
    redis_db,redis_regex = get_redis_db(name)
    redis_host,redis_user,redis_pass = get_user_password(name)
    #print(f'REDIS hostname : {host} username : {db_user} - password : {db_pass}')


    json_data = {
    "name": f"{name}",
    "config": {
      "connector.class": "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
      "tasks.max": "1",
      "redis.hosts": f"{redis_host}",
      "redis.password": f"{redis_pass}",
      "redis.database": f"{redis_db}",
      "topics.regex": f"{redis_regex}.*",
      "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
      "value.converter" : "org.apache.kafka.connect.storage.StringConverter",
      "errors.log.enable" : "true",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
        }
    }

    create_connector = requests.post('http://localhost:8083/connectors', json=json_data)
    print("status code: ", create_connector.status_code)
    print("repose:")
    print(create_connector.json())
    #print(json_data)

def update_redis_sink(name):
    redis_db,redis_regex = get_redis_db(name)
    redis_host,redis_user,redis_pass = get_user_password(name)
    #print(f'REDIS hostname : {host} username : {db_user} - password : {db_pass}')
    
    json_data = {
      "connector.class": "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
      "tasks.max": "1",
      "redis.hosts": f"{redis_host}",
      "redis.password": f"{redis_pass}",
      "redis.database": f"{redis_db}",
      "topics.regex": f"{redis_regex}.*",
      "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
      "value.converter" : "org.apache.kafka.connect.storage.StringConverter",
      "errors.log.enable" : "true",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }

    update_connector = requests.put(f'http://localhost:8083/connectors/{name}/config', json=json_data)
    print("status code: ", update_connector.status_code)
    print("repose:")
    print(update_connector.json())
    #print(json_data)



if __name__ == '__main__':
    check_existing(os.environ['CONNECTOR_NAME'])
