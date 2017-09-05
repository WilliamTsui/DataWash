#!-*-coding=utf-8-*-
import MySQLdb

class ConnDB(object):
    
    def __init__(self,SqlInfo):
        self.sql=SqlInfo
        
    def ConnMysql(self):
        try:
            conn=MySQLdb.connect(host="localhost",user="chief",passwd="chief",db="datawash",charset="utf8")
            cursor=conn.cursor()
            cursor.execute(self.sql)
            result=cursor.fetchall()
            return result
            conn.close()
        except MySQLdb.Error,e:
            return e    
if __name__ == '__main__':
    pass