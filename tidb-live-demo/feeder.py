#!/usr/bin/env python3
from datetime import datetime
import json
import os
import time
import yaml
import pymysql

CONFIG_FILE = 'app.yaml'
INPUT_FILE = 'data/input.jsonl'

config = yaml.load(open(CONFIG_FILE), Loader=yaml.BaseLoader)
env = config['env_variables']
db_user = env['SQL_USERNAME']
db_password = env['SQL_PASSWORD']
db_port = int(env['SQL_PORT'])
db_address = env['SQL_ADDRESS']
db_name = env['SQL_DB_NAME']


DROP_TABLE = 'DROP TABLE IF EXISTS pr;'
CREATE_TABLE = '''
CREATE TABLE IF NOT EXISTS pr (
    id INT AUTO_INCREMENT PRIMARY KEY,
    insertTime VARCHAR(255),
    createdAt VARCHAR(255),
    author VARCHAR(255),
    url VARCHAR(255),
    title VARCHAR(1024),
    repo VARCHAR(255),
    owner VARCHAR(255),
    additions INT,
    deletions INT,
    generation INT
);
'''
INSERT_STMT = '''
INSERT INTO pr(insertTime, createdAt, author, url, title, repo, owner, additions, deletions, generation)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
DELETE_STMT = '''
DELETE FROM pr WHERE generation < %s
'''

cnx = pymysql.connect(user=db_user, password=db_password,
    host=db_address, db=db_name, port=db_port, autocommit=True)
with cnx.cursor() as cursor:
    cursor.execute(DROP_TABLE)
    cursor.execute(CREATE_TABLE)
    while True:
        generation = 1
        for line in open(INPUT_FILE):
            print(line)
            record = json.loads(line)
            args = (
                datetime.now().isoformat(),
                record.get('createdAt'),
                record.get('author'),
                record.get('url'),
                record.get('title'),
                record.get('repo'),
                record.get('owner'),
                record.get('additions'),
                record.get('deletions'),
                generation
            )
            time.sleep(0.5)
            cursor.execute(INSERT_STMT, args)
        generation += 1
        cursor.execute(DELETE_STMT, (generation - 2,))
