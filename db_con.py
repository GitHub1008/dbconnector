"""

https://stackoverflow.com/questions/44765482/multiple-constructors-the-pythonic-way

"""


import pymysql
import json
import snowflake.connector


class dbms():
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.user = kwargs['user']
        self.pwd = kwargs['pwd']
    #
    # @classmethod
    # def dbcon(cls, account, user, pwd) -> 'dbms':
    #     return cls(host=account, user=user, pwd=pwd)

    def connect_db(self):
        pass

    def close(self):
        pass

    def validate(self):
        pass

class mysqldb(dbms):
    def __init__(self, port, **kwargs):
        super().__init__(**kwargs)
        self.port = port

    def connect_db(self):
        try:
            self.con = pymysql.connect(user=self.user,
                                    password=self.pwd,
                                    host=self.host,
                                    port=self.port)
            return self.con

        except Exception as e:
            print(f"ERROR OCCUR:: {e}")
            raise e


    def close(self):
        return self.con.close()

    def validate(self):
        connection = self.connect_db()
        try:
            cur = connection.cursor()
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            print(version)
        except Exception as e:
            print(f"ERROR OCCUR:: {e}")
        finally:
            connection.close()

class snowflakedb(dbms):
    def connect_db(self):
        self.con = snowflake.connector.connect(user = self.user,
                                password = self.pwd,
                                account = self.host)

        cur = self.con.cursor()
        version = cur.execute('select current_version();').fetchone()[0]
        print(version)
        return self.con

    def close(self):
        return self.con.close()

    def validate(self):

        con = snowflake.connector.connect


if __name__ == "__main__":
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    mysqlcon = mysqldb(host=dbinfo['MYSQL_HOST'],
                  user=dbinfo['MYSQL_USER'],
                  pwd=dbinfo['MYSQL_PWD'],
                  port=dbinfo['MYSQL_PORT'])
    mysqlcon.connect_db()
    mysqlcon.validate()
    mysqlcon.close()

    snowflakecon = snowflakedb(user=dbinfo['SF_USER'],
                               pwd=dbinfo['SF_PWD'],
                               host=dbinfo['SF_ACCOUNT'])
    snowflakecon.connect_db()


