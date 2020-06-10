#-*- coding: utf-8 -*-
from pymysql import *
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

# 商品id变化范围
N=50
# 本地txt
matrix_path = '/Users/harry/Desktop/test_data.txt'
# sql密码
sqlpassword = "Reborn2018"

# mysql相关操作
class Mysqlpython:
    def __init__(self,database,host="localhost",
        user="root",password=sqlpassword,
        charset="utf8",port=3306):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.charset = charset
        self.port = port
    # 创建数据连接和游标对象
    def open(self):
        self.db = connect(host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=self.database,
            charset=self.charset)
        self.cur = self.db.cursor()

    # 关闭游标对象和数据库连接对象
    def close(self):
        self.cur.close()
        self.db.close()

    # 执行sql命令
    def zhixing(self,sql,L=[]):
        self.open()

        self.cur.execute(sql,L)
        self.db.commit()

        self.close()

    # 查询功能
    def all(self,sql,L=[]):
        self.open()
        self.cur.execute(sql,L)
        result = self.cur.fetchall()
        return result

    # 插入功能
    def insert(self,sql,L=[]):
        self.open()
        self.cur.execute(sql,L)
        self.db.commit()

    def delete(self,L=[]):
        self.open()
        self.cur.execute("TRUNCATE TABLE result;",L)
        self.db.commit()


# 主函数
if __name__ == "__main__":
    # 此处输入替换为本地数据库名
    sqlh = Mysqlpython("recommend") 
    # 打印当前内容
    sel = "select * from result"
    r = sqlh.all(sel)
    print(r)
    
    fr=open(matrix_path)
    sqlh.delete()
    count = 0
    recommend = np.zeros((N,1))
    for line in fr.readlines():
        #逐行读取
        lineArr=line.strip().split(',')
        print(lineArr)
        sql = "INSERT INTO result (user_name,recommend1,recommend2,recommend3) VALUES ("+ str(lineArr[0]) +","+str(lineArr[1])+","+str(lineArr[2])+","+str(lineArr[3])+");"
        print(sql)
        sqlh.insert(sql)
        count += 1
        # 统计商品被推荐次数
        recommend[int(lineArr[1])] += 1
        recommend[int(lineArr[2])] += 1
        recommend[int(lineArr[3])] += 1


# 可视化
y= []
for i in range(N):
    y.append(recommend[i][0])
index = np.arange(N)
pl = plt.bar(x=index,height=y,color='blue',width=0.8)
plt.xlabel('item id')
plt.ylabel('recommended times')
plt.show()