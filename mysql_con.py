import pymysql
import json
import psycopg2

def maria_connector():
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    try:
        con = pymysql.connect(host=dbinfo["MYSQL_HOST"]
                              , user=dbinfo["MYSQL_USER"]
                              , password=dbinfo["MYSQL_PWD"]
                              , database=dbinfo["MYSQL_DB"]
                              , port=dbinfo["MYSQL_PORT"])
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        version = cur.fetchone()[0]
        print(version)
        return con

    except Exception as e:
        print(e)

def mysql_connector():
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    try:
        con = pymysql.connect(host=dbinfo["MYSQL_HOST"]
                              , user=dbinfo["MYSQL_USER"]
                              , password=dbinfo["MYSQL_PWD"]
                              , database=dbinfo["MYSQL_DB"]
                              , port=dbinfo["MYSQL_PORT"])
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        version = cur.fetchone()
        print(version)
        return con

    except Exception as e:
        print(e)

def postgres_connector():
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    try:
        con = psycopg2.connect(host=dbinfo["POSTGRES_HOST"]
                              , user=dbinfo["POSTGRES_USER"]
                              , password=dbinfo["POSTGRES_PWD"]
                              , database=dbinfo["POSTGRES_DB"]
                              , port=dbinfo["POSTGRES_PORT"])
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        version = cur.fetchone()
        print(version)
        return con

    except Exception as e:
        print(f"Exception occur:{e}")

if __name__ == "__main__":
    postgres_connector()