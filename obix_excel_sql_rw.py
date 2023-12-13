from datetime import datetime
from oBIX import Client
import pandas as pd
from threading import Thread
import numpy as np
import schedule
import pymysql
import math
from config import MYSQL_CONFIG,OBIX_CONFIG

device_list=['温度传感器','压力传感器','阀门','地源泵','用户泵','锅炉循环泵','氢燃料电池预热循环泵','蓄水箱循环泵','蓄水箱循环泵','补水泵','主循环泵','主循环泵','进地热井','蓄水箱液位','补水箱液位','蓄能水箱压力监测',
             '出蓄水箱总管温度','出蓄水箱总管压力','能源站回水管除污器前压力监测','能源站回水管除污器后压力监测','接DK-1及DK-2地块供水管压力监测','接DK-1及DK-2地块供水管温度监测','锅炉回水总管温度监测','锅炉回水总管压力监测','旁通管压差监测',
             '补水泵阀门','电表','出地热井水管总管水管冷热量监测','能源站供水管冷热量监测','接DK-1及DK-2地块供水管冷热量监测','氢燃料电池余热冬季蓄热时出水管冷热量监测','地源热泵夏季夜间往蓄水罐蓄冷时出水管冷热量监测',
             '电锅炉冬季蓄热时出水管冷热量监测','进地热井水管总管流量监测','地源热泵夏季夜间往蓄水罐蓄冷时供水管冷热量监测','地源热泵蒸发侧流量','地源热泵地源侧流量','水表','地源热泵','水泵','锅炉','平台控制相关点位',
             '能源中心板换','消防水池板换','运动员村','氢燃料电池','水箱温度','水箱液位']


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
# obix_thread函数执行一遍，就得到tmp表的每行解析出来的reate_time,addr,name,unit,value
def obix_thread():
    #client = Client("192.168.6.60", "obix", "Honeywell2022", port = 8082, https=False)
    client = Client(**OBIX_CONFIG)
    #这个循环会把都tmp表的每一行都遍历一边
    global load_time

    i=0
    while(len(tmp)>0): 

        i=i+1
        point=tmp.pop(0)    #这里的pop也是填充过了的
        system_id= '10'       # 1
        system_name= '榆林能源站暖通系统'     #暖通系统
        #device_string = point[0]
        #print("device_string:",device_string,type(device_string))
        # 0设备编号	 1点位地址	2点位类型	3读写权限	4注释	Unnamed: 5	Unnamed: 6	7device_id	8device_name	9device_id_00	10position_id    11position_id_database
        try:
            device_id = system_id+ f"{point[9]:02}"   
            device_name = point[8]
            position_id = device_id+f"{point[10]:06}"  
            position_name=point[4]
        except Exception as e:   
            print(f"此设备有问题: {point}, Error: {e}")   #有些写不进去 

        # unit=str(point[5]) if len(str(point[5]))<100 else str(point[5])[:100]
        load_value=read_obix(client,'/config/Drivers/NiagaraNetwork/YL_8000/points'+point[1])   # 这个相当于是读取表里面的地址，而不是一股脑的把全部数据都采集上来
        print("load_value:",load_value,type(load_value),i)

        dl.append([load_time,system_id,system_name,device_id,device_name,position_id,position_name,load_value])
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
    tmp=pd.read_excel(io='/home/ems/ems_capture/obix_capture-main/processed_obix_database.xlsx',sheet_name=0)
    # 使用前向填充方法填充 NaN 值（向前填充）
    #tmp = tmp.fillna(method='ffill', axis=0)
    tmp=np.array(tmp).tolist()




if __name__ == '__main__':
    
    global tmp
    tmp=pd.read_excel(io='/home/ems/ems_capture/obix_capture-main/processed_obix_database.xlsx',sheet_name=0)
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

