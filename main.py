# 读取json 文件，实现数据入库，增删改查
# 读取medical.json信息,原有8808条数据,不能用with直接打开，因为行数太长了直接读出来会报错
import json

import pymysql
from pymysql import connect

medical_json_path = './medical.json'

#连接mysql数据库，表名为medical
def createMysqlConn():
    try:
        conn = connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='root',
            db='medical',
            charset='utf8')
        print("数据库连接成功，连接对象基本信息如下：")
        print(conn)
        print(type(conn))
        print("\n")
        # cursor = conn.cursor()
        return conn
    except Exception as e:
        print(e)

def list2str(data):
    data_after=",".join(str(i) for i in data)
    return data_after

# 读取json 数据
def read2mysql(conn,medical_json_path):

    diseases = []
    disease_infos = []
    count = 0
    i=0
    for data in open(medical_json_path, encoding='utf-8'):
        disease_dict = {}
        count += 1
        i += 1
        data_json = json.loads(data)
        disease = data_json['name']
        diseases.append(disease)
        disease_dict['d'] = i
        disease_dict['disease_id'] = data_json['_id']['$oid']
        disease_dict['name'] = data_json['name']
        disease_dict['cause'] = data_json['cause']
        # 症状要特殊处理，因为其本身是列表要将其转为字符串
        disease_dict['symptom'] = list2str(data_json['symptom'])
        disease_dict['desc'] = data_json['desc']
        #可能原数据中不存在这两个字段get_way/cost_money
        # if 'get_way ' in data_json:
        #     disease_dict['get_way'] = data_json['get_way']
        # else:
        #     disease_dict['get_way']=''
        # if 'cost_money ' in data_json:
        #     disease_dict['cost_money'] = data_json['cost_money']
        # else:
        #     disease_dict['cost_money'] = ''

        # 将disease_dict装入disease_infos
        disease_infos.append(disease_dict)

        item_values = list(disease_dict.values())
        sql_value_list = []
        # pymysql.escape_string(product_name)不符合规范
        # pymysql.converters.escape_string()新规范
        for sql_item in item_values:
            #一般只有id 是int类型
            if type(sql_item) == int:
                sql_item = str(sql_item)
            sql_item = pymysql.converters.escape_string(sql_item)
            sql_value_list.append(sql_item)
        param = tuple(sql_value_list)
        #入库
        #sql1=("INSERT IGNORE INTO disease(disease_id,name,cause,symptom,desc,get_way,cost_money)VALUES(%s,%s,%s,%s,%s,%s,%s)")
        sql1 = ("INSERT IGNORE INTO disease VALUES(%s,%s,%s,%s,%s,%s)")
        try:
            cursor=conn.cursor()
            cursor.execute(sql1, param)
            conn.commit()
            print(str(i) + " 写入mysql成功!")
        except(IndexError):
            print(str(i) + "IndexError")
            continue
        except(UnicodeEncodeError):
            print(str(i) + "UnicodeEncodeError")
            continue
        except(TypeError):
            print(str(i)+"TypeError")
        except(KeyError):
            print(str(i) + "KeyError")
    print("入库完毕")
#每条记录有6个属性
#增加单条记录，每一个属性都要有
def add(conn,param):
    sql_add = ("INSERT IGNORE INTO disease VALUES(%s,%s,%s,%s,%s,%s)")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_add, param)
        conn.commit()
        print(" 写入mysql成功!")
        cursor.close()
        conn.close()
    except(KeyError):
        print("KeyError")
    return
#按疾病的id删除单条记录，参数化删除
def delete(conn,param):
    sql_delete= (" DELETE FROM disease where id = %s ")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_delete,param)
        conn.commit()
        print(" 删除")
        cursor.close()
        conn.close()
    except(KeyError):
        print("KeyError")
    return

#更改记录，参数化修改
def update(conn,param):
    sql_update= ("update disease set name = %s where id = %s")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_update, param)
        conn.commit()
        print("更改成功")
        cursor.close()
        conn.close()
    except(KeyError):
        print("KeyError")
    return
#查找记录，参数查找
def find(conn,param):
    sql_find = ("SELECT * FROM disease WHERE id = %s ")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_find, param)
        data = cursor.fetchone()
        print(data)
        print("查找成功")
        cursor.close()
        conn.close()
    except(KeyError):
        print("KeyError")
    return





if __name__ =='__main__':
    mysqlConn = createMysqlConn()
    #入库
    # read2mysql(mysqlConn,medical_json_path)
    op=input("请输入操作：")
    if op =='add':
        id,disease_id,name,cause,symptom,desc=input("请输入新记录的id,disease_id,name,cause,symptom,desc:").split(" ")
        param=(id,disease_id,name,cause,symptom,desc)
        add(mysqlConn,param)
    elif op == 'delete':
        id=input("请输入待删除的记录的id：")
        param =tuple(id)
        delete(mysqlConn,param)
        # print(id)
    elif op =='find':
        param=input("请输入待查找记录的id：")
        find(mysqlConn,param)
    elif op =='update':
        new,id=input("请输入新的值和索引值：").split(" ")
        # index=int(index)
        param=(new,id)
        update(mysqlConn,param)



