from pymysql import TIME
from mysql import Mysql
import pandas as pd
import numpy as np
import time


class DataMysql(Mysql):

    '''初始化'''
    def __init__(self):
        super(DataMysql, self).__init__()
        self.IAQI = None
        self.hours_columns = ['TIME', 'dissolved_od_y', 'nh3n_y', 'Cd_y', 'bg_algae_y', 'conductivity_y', 'Cu_y','chlorophyll_y','TDS','COD','turbidity','WATER_TEMPER_y','ph',
                              'wind_direction', 'wind_speed', 'pm1', 'rainfall', 'illumination','ship_pm25','air_pressure','so2','ship_pm10','air_temper', 'ship_humid', 'co', 'ship_state',
                              'pm1_n', 'pm25_n', 'pm10_n', 'co2_n', 'co_n', 'o3_n', 'so2_n','no2_n','humid_n','h2s_n','ch4_n','temper_n',
                              'dissolved_6001', 'nh3n_6001', 'bg_algae_6001', 'conductivity_6001','chlorophyll_6001','turbidity_6001','ph_6001','WATER_TEMPER_6001','ship_humid_6001','air_temper_6001','co2_6001','NH3_6001']

    '''读取hours_900_600的数据'''
    def FindMaxData(self):
        sql = "SELECT * FROM hours_900_600 ORDER BY id DESC LIMIT 1;"
        self.SqlExecute(sql)
        if None == self.cursor:
            return [-1 for i in range(len(self.hours_columns)-1)]
        data = self.cursor.fetchone()
        self.Close()
        if None == data:
            return [-1 for i in range(len(self.hours_columns)-1)]
        return (list(data[2:51]))
    # '''读取hours_all_update的云阳水质数据'''
    # def FindMaxData(self):
    #     sql = "SELECT * FROM hours_all_update ORDER BY id DESC LIMIT 1;"
    #     self.SqlExecute(sql)
    #     # print(self.SqlExecute(sql))
    #     if None == self.cursor:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     data=self.cursor.fetchone()
    #     self.Close()
    #     if None == data:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     return list(data[2:14])

    #     '''读取hours_all_update的云阳气象数据'''
    # def FindMaxData1(self):
    #     sql = "SELECT * FROM hours_all_update ORDER BY id DESC LIMIT 1;"
    #     self.SqlExecute(sql)
    #     # print(self.SqlExecute(sql))
    #     if None == self.cursor:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     data=self.cursor.fetchone()
    #     self.Close()
    #     if None == data:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     return list(data[14:26])

    #     '''读取hours_all——update的大气数据（南山片区）'''
    # def FindMaxData2(self):
    #     sql = "SELECT * FROM hours_all_update ORDER BY id DESC LIMIT 1;"
    #     self.SqlExecute(sql)
    #     # print(self.SqlExecute(sql))
    #     if None == self.cursor:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     data=self.cursor.fetchone()
    #     self.Close()
    #     if None == data:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     return list(data[26:38])
    #     '''读取hours_all——update的9001船状态数据'''
    # def FindMaxData3(self):
    #     sql = "SELECT * FROM hours_all_update ORDER BY id DESC LIMIT 1;"
    #     self.SqlExecute(sql)
    #     # print(self.SqlExecute(sql))
    #     if None == self.cursor:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     data=self.cursor.fetchone()
    #     self.Close()
    #     if None == data:
    #         return [-1 for i in range(len(self.hours_columns)-1)]
    #     return list(data[38:])
    '''提取主界面'''
    def FindMainData(self):
        res = []
        data1 = self.__GetAllAQIData('iaqi')
        data2 = self.FindMaxData()
        res.append(self.__GetAqiRank(data1.mean().loc['aqi']))
        # print("*"*25 + "bebug" + "*"*25)
        # print(data1.drop(['TIME',"aqi"],axis=1).mean())
        if data1.empty:
            res.append("none")
        else:
            res.append(data1.drop(['TIME', "aqi"], axis=1).mean().idxmax())
        res.append(data2[-2])
        res.append(data2[-1])
        return res

    '''vis接口'''
    def FindVisData(self):
        res = {
            'aqi':[],
            'poll':[]
        }
        data = self.__GetAllAQIData('aqi')  # 获取全部的数据
        if data.empty:
            result = {
                'aqi':res['aqi'],
                'poll':{
                    'name':'none',
                    'data':res['poll']
                }
            }
            return result
        max_poll = data.drop(['TIME', "aqi"], axis=1).copy()
        time_list = list(data['TIME'])
        for i in range(24):   # 0-23
            if i not in time_list:
                res['aqi'].append(-1)
                res['poll'].append(-1)
            else:
                res['aqi'].append(int(data[data['TIME'] == float(i)]['aqi']))
                res['poll'].append(int(data[data['TIME'] == float(i)][max_poll.mean().idxmax()]))
        result = {
            'aqi': res['aqi'],
            'poll': {
                'name': max_poll.mean().idxmax(),
                'data': res['poll']
            }
        }
        return result

    '''获取表格全部数据'''
    def __GetAllAQIData(self, table_name):
        sql = "select * from " + table_name
        data = np.empty((0, len(self.aqi_cloumns)))
        global value
        if self.SqlExecute(sql):
            value = self.cursor.fetchone()
            if None == value:
                df = pd.DataFrame(columns=self.aqi_cloumns, data=data)
                return df
        while value is not None:
            res = [[float(value[i]) for i in range(0, len(self.aqi_cloumns))]]
            data = np.append(data, res, axis=0)
            value = self.cursor.fetchone()
        df = pd.DataFrame(columns=self.aqi_cloumns, data=data)
        return df

    def __GetAqiRank(self, value):
        index = 0
        for i in range(len(self.IAQI)):
            if value >= self.IAQI[i]:
                index = i
            else:
                break
        return index

if __name__ == "__main__":
    mysql = DataMysql()
    print(mysql.FindMaxData())

    #创建小时表
    # sql = """CREATE TABLE hours(id integer primary key auto_increment,
    #                             TIME CHAR(20),
    #                             pm25 float,
    #                             pm10 float,
    #                             so2 float,
    #                             co float,
    #                             no2 float,
    #                             o3 float,
    #                             o2 float,
    #                             co2 float,
    #                             ch4 float,
    #                             h2s float,
    #                             humid float,
    #                             temper float)"""
    # if 1 == mysql.SqlExecute(sql):
    #     print("创建成功")
    #     mysql.Close()
    # else:
    #     print("创建失败")

    #创建IAQI表
    # sql = """CREATE TABLE aqi( TIME CHAR(20) not null primary key,
    #                             aqi float,
    #                             pm25 float,
    #                             pm10 float,
    #                             so2 float,
    #                             co float,
    #                             no2 float,
    #                             o3 float)"""
    # if 1 == mysql.SqlExecute(sql):
    #     print("创建成功")
    #     mysql.Close()
    # else:
    #     print("创建失败")



    # data = {'co': 30.0, 'o2': 10.0, 'ch4': 16.0, 'o3': 280.0, 'h2s': 10, 'so2': 1610, 'nh3': 12, 'no2': 120.0, 'no': 5.0, 'pm1': 26.0, 'pm10': 220, 'pm25': 260, 'humid': 0.0, 'temper': 28.5, 'co2': 620.0}
    # mysql.StorageTestData(data)
    # data2 = {'co': 20.0, 'o2': 10.5, 'ch4': 20.0, 'o3': 40.0, 'h2s': 20, 'so2': 20, 'nh3': 12, 'no2': 40.0, 'no': 0.0, 'pm1': 0.0, 'pm10': 3, 'pm25': 11, 'humid': 0.0, 'temper': 26.5, 'co2': 2.0}
    # mysql.StorageTestData(data2)
    # data3 = {'co': 20.0, 'o2': 10.5, 'ch4': 20.0, 'o3': 40.0, 'h2s': 20, 'so2': 20, 'nh3': 12, 'no2': 40.0, 'no': 0.0, 'pm1': 0.0, 'pm10': 2, 'pm25': 12, 'humid': 0.0, 'temper': 26.5, 'co2': 2.0}
    # mysql.StorageTestData(data3)
    # data4 = {'co': 20.0, 'o2': 10.5, 'ch4': 20.0, 'o3': 40.0, 'h2s': 20, 'so2': 20, 'nh3': 12, 'no2': 4.0, 'no': 0.0, 'pm1': 0.0, 'pm10': 3, 'pm25': 11, 'humid': 0.0, 'temper': 26.5, 'co2': 2.0}
    # mysql.StorageTestData(data4)
    # data5 = {'co': 20.0, 'o2': 10.5, 'ch4': 20.0, 'o3': 40.0, 'h2s': 20, 'so2': 20, 'nh3': 12, 'no2': 4.0, 'no': 0.0, 'pm1': 0.0, 'pm10': 2, 'pm25': 12, 'humid': 0.0, 'temper': 26.5, 'co2': 2.0}
    # mysql.StorageTestData(data5)
    # data6 = {'co': 20.0, 'o2': 10.5, 'ch4': 20.0, 'o3': 40.0, 'h2s': 20, 'so2': 20, 'nh3': 12, 'no2': 4.0, 'no': 0.0, 'pm1': 0.0, 'pm10': 4, 'pm25': 10, 'humid': 0.0, 'temper': 26.5, 'co2': 2.0}
    # mysql.StorageTestData(data6)
    # print(mysql.FindMaxData())
    # print("1")
    # print(mysql.FindAQIData())
    # print("2")
    # print(mysql.FindPollData())
    # print("3")
    # print(mysql.FindVisData())
    # print("4")
    # print(mysql.FindMainData())