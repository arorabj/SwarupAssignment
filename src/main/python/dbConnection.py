import psycopg2 as p
import configparser as cp

confPath = '../../../resources/config/application.properties'
env = "dev"

#get variables from config File
props = cp.RawConfigParser()
props.read(confPath)

def read_config(paramtr : str):
    return props.get(env, paramtr)

def db_connection_open():
    try:
        mode = read_config('dataWriteMode')
        url_connect = read_config('urlConnect')
        properties = read_config('connProperties')
        conn = p.connect("host=127.0.0.1 dbname=testdb user=udacity password=test1234")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except p.Error as e:
        print("connection failed")
        print (e)
    else:
        return conn, cur


def db_connection_close(conn, cur):
    try:
        cur.close()
        conn.close()
    except p.Error as e:
        print("connection close failed")
        print (e)