import pymysql
import json

class dbms():
    def __init__(self, host, user, pwd, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port

class mysql(dbms):
    def connect_db(self):
        try:
            self.con = pymysql.connect(user=self.user,
                                    password=self.pwd,
                                    host=self.host,
                                    port=self.port)
            return self.con

        except Exception as e:
            print(f"ERROR OCCUR:: {e}")


    def close(self):
        self.con.close()

    def validate(self):
        connection = self.connect_db()
        try:
            cur = connection.cursor()
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            print(version)
        except Exception as e:
            print(f"ERROR OCCUR:: {e}")





if __name__ == "__main__":
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    mycon = mysql(host=dbinfo['MYSQL_HOST'],
                  user=dbinfo['MYSQL_USER'],
                  pwd=dbinfo['MYSQL_PWD'],
                  port=dbinfo['MYSQL_PORT'])
    mycon.connect_db()
    mycon.validate()
    mycon.close()

