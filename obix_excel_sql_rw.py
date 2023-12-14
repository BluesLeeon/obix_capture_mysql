from datetime import datetime
from oBIX import Client
import pandas as pd
from threading import Thread
import numpy as np
import schedule
import pymysql
import math
from config import MYSQL_CONFIG,OBIX_CONFIG


# 给定客户端和地址，就可以读地址的数据  注意插入的数值不能为nan
#读取到的有些值为bool类型，要转为float
def read_obix(client, point_path):
    point_value = client.read_point_value(point_path)
    if isinstance(point_value, float) and not math.isnan(point_value):
        point_value = round(point_value, 10)  # 保留十位小数
    elif isinstance(point_value, bool):
        point_value = float(point_value)  # 将布尔值转换为相应的数值类型，比如 1 或 0
    # else:
    #     point_value=0
    return point_value





# 要处理这个表，根究表的信息，给出[load_time,system_id,system_name,device_id,device_name,position_id,position_name,load_value]
# obix_thread函数执行一遍，就得到tmp表的每行解析出来的create_time,addr,name,unit,value
def obix_thread():
    #client = Client("192.168.6.60", "obix", "Honeywell2022", port = 8082, https=False)
    client = Client(**OBIX_CONFIG)
    #这个循环会把都tmp表的每一行都遍历一边
    global load_time
    i=0
    while(len(tmp)>0): 

        i=i+1
        #tmp: 0设备编号	1点位地址	2点位类型	3system_id	4system_name	5device_id	6device_name	7position_id	8position_name	9position_name_explain1	10position_name_explain2	11position_id_database
        point=tmp.pop(0)    #这里的pop也是填充过了的 取df的第一行
        system_id = f"{point[3]:04}"   
        system_name = point[4]
        device_id = f"{point[5]:04}" 
        device_name = point[6]
        position_id_database = f"{point[11]:014}" 
        position_name= point[8]
        load_value=read_obix(client,'/config/Drivers/NiagaraNetwork/YL_8000/points'+point[1])   # 这个相当于是读取表里面的地址，而不是一股脑的把全部数据都采集上来
        print("load_value:",load_value,type(load_value),i)
        dl.append([load_time,system_id,system_name,device_id,device_name,position_id_database,position_name,load_value])
    print(len(dl))


def duqu1():
    global dl
    global tmp
    global load_time
    # obix_thread()

    load_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")  #所有5分钟的倍数加入到列表中 到点就运行
    #load_time = datetime.now().replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    t_l=[]
    for i in range(4):
        obix_t=Thread(target=obix_thread,args=())
        t_l.append(obix_t)
    # post_t=Thread(target=postgre_write_thread,args=())
    for t in t_l:
        t.start()
    for t in t_l:
        t.join()


    print("准备开始：将数据数据传入数据库之中") 
    # 连接数据库，并写值进去
    mysql_connection = pymysql.connect(**MYSQL_CONFIG)
    for d in dl:
        if d[7] !=None:
            cursor = mysql_connection.cursor()
            sql = 'INSERT INTO ems_hvac_history (load_time, system_id, system_name, device_id, device_name, position_id, position_name, load_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            try:
                cursor.execute(sql, (d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7]))
            except Exception as e:   
                print(f"Unable to insert data: {d}, Error: {e}")   #有些写不进去 

    mysql_connection.commit()
    mysql_connection.close()
    print("已经将数据数据传入数据库之中")  # 4203  这一步没过来


    dl=[]
    tmp=pd.read_excel(io='/home/ems/ems_capture/obix_capture-main/processed_obix_database_v2.xlsx',sheet_name=0)
    # 使用前向填充方法填充 NaN 值（向前填充）
    #tmp = tmp.fillna(method='ffill', axis=0)
    tmp=np.array(tmp).tolist()




if __name__ == '__main__':
    
    global tmp
    tmp=pd.read_excel(io='/home/ems/ems_capture/obix_capture-main/processed_obix_database_v2.xlsx',sheet_name=0)
    # 使用前向填充方法填充 NaN 值（向前填充）
    # tmp = tmp.fillna(method='ffill', axis=0)   #
    tmp=np.array(tmp).tolist()

    global dl
    dl=[]

    global load_time
    load_time = None

    # 将所有5分钟的倍数加入到列表中
    times = ["{:02d}:{:02d}".format(hour, minute) for hour in range(0, 24) for minute in range(0, 60, 5)]

    #schedule.every().minute.at(":05").do(duqu1)
    #schedule.every(5).minutes.do(duqu1)

    # 设置定时任务
    for t in times:
        schedule.every().day.at(t).do(duqu1)
    while True:
        schedule.run_pending()

