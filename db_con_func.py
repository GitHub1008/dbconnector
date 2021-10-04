import pymysql
import json
import psycopg2
import snowflake.connector
from sqlalchemy import create_engine, text

def maria_connector():
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    try:
        con = pymysql.connect(host=dbinfo["MARIA_HOST"]
                              , user=dbinfo["MARIA_USER"]
                              , password=dbinfo["MARIA_PWD"]
                              , database=dbinfo["MARIA_DB"]
                              , port=dbinfo["MARIA_PORT"])
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
        cur.execute("select @@version;")
        version = cur.fetchone()[0]
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


def snowflake_connector():
    try:
        with open('dbinfo.json') as fp:
            dbinfo = json.loads(fp.read())

        con = snowflake.connector.connect(user=dbinfo['SF_USER'],
                                    password=dbinfo['SF_PWD'],
                                    account=dbinfo['SF_ACCOUNT'])
        cur = con.cursor()
        cur.execute("SELECT current_version();")
        version = cur.fetchone()[0]
        print(f"version of snowflake: {version}")
        return con, version
    except Exception as e:
        print(f"Exception occur:{e}")


def sqlalchemy_connector():
    try:
        with open('dbinfo.json') as fp:
            dbinfo = json.loads(fp.read())

        sf_idenfier = f"snowflake://{dbinfo['SF_USER']}:{dbinfo['SF_PWD']}@{dbinfo['SF_ACCOUNT']}"
        mysql_idenfier = f"mysql+pymysql://{dbinfo['MYSQL_USER']}:{dbinfo['MYSQL_PWD']}@{dbinfo['MYSQL_HOST']}:{dbinfo['MYSQL_PORT']}/{dbinfo['MYSQL_DB']}"
        engine = create_engine(f"{mysql_idenfier}", echo=False, encoding='utf-8')
        con = engine.connect()
        # sf_version = con.execute("SELECT current_version();").fetchone()[0]
        mysql_version = con.execute("SELECT VERSION();").fetchone()[0]
        print(mysql_version)
    except Exception as e:
        print(e)

    finally:
        con.close()
        engine.dispose()
        print("sqlalchemy connection close")



if __name__ == "__main__":
    # maria_connector()
    mysql_connector()
    # postgres_connector()
    # snowflake_connector()
    # sqlalchemy_connector()


"""
References

https://docs.snowflake.com/en/user-guide/sqlalchemy.html 
"""