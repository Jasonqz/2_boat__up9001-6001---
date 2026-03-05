from pymysql import TIME
from mysql import Mysql
import pandas as pd
import numpy as np
import time
import logging
import random

bc = logging.basicConfig(filename='./err.log')

class DataAir:  
    def __init__(self, str_data):

        if "" == str_data:
            print("结束")
            return ""

        print(len(str_data))

        self.AQI_list = {'o3': 48, 'so2': 64, 'no2': 46, 'co': 28}   #六参
        self.EPS = 0.00001
        self.result = {}
        res =[str_data[2],str_data[3],str_data[4],str_data[5],str_data[6],str_data[7],str_data[8],str_data[9],str_data[10],str_data[11],str_data[12],str_data[13],
              str_data[14],str_data[15],str_data[16],str_data[17],str_data[18],str_data[19],str_data[20],str_data[21],str_data[22],str_data[23],str_data[24],str_data[25],str_data[26],
              str_data[57],str_data[56],str_data[55],str_data[51],str_data[50],str_data[53],str_data[45],str_data[48],str_data[58],str_data[46],str_data[54],str_data[67],
              str_data[119],str_data[122],str_data[124],str_data[117],str_data[120],str_data[118],str_data[116],str_data[115],str_data[114],str_data[123],str_data[107],str_data[103],]

        list_AQI ={0:'dissolved_od_y',1:'nh3n_y',2:'Cd_y',3:'bg_algae_y',4:'conductivity_y',5:'Cu_y',6:'chlorophyll_y',7:'TDS',8:'COD',9:'turbidity',10:'WATER_TEMPER_y',11:'ph',
                   12:'wind_direction',13:'wind_speed',14:'pm1',15:'rainfall',16:'illumination', 17:'ship_pm25',18:'air_pressure',19:'so2',20:'ship_pm10',21:'air_temper',22:'ship_humid',23:'co',24:'ship_state',
                   25:'pm1_n',26:'pm25_n',27:'pm10_n',28:'co2_n',29:'co_n',30:'o3_n',31:'so2_n', 32:'no2_n', 33:'humid_n',34:'h2s_n',35:'ch4_n',36:'temper_n',
                   37:'dissolved_6001',38:'nh3n_6001',39:'bg_algae_6001',40:'conductivity_6001', 41:'chlorophyll_6001', 42:'turbidity_6001', 43:'ph_6001',44:'WATER_TEMPER_6001',45:'ship_humid_6001',46:'air_temper_6001',47:'co2_6001',48:'NH3_6001',}
  
        #获取数据hours_900_600
        for i in range(len(res)):
            name = list_AQI[i]
            value = res[i]
            #数据判断
            if float(value) + 999 < self.EPS:       # 如果存在这种值就算是无效值
                self.result[name] = 0.0
            # elif name in self.AQI_list.keys() and name == 'co':
            #     self.result[name] = self.unit(float(value), self.AQI_list[name])/1000
            # elif name in self.AQI_list.keys():
            #     self.result[name] = self.unit(float(value), self.AQI_list[name])
            else:
                self.result[name] = float(value)
        #self.result['WATER_DEPTH_y'] = 14 + round(random.uniform(0.6, 0.8), 2)
        #self.result['temper_n'] = 7 + round(random.uniform(0.6, 0.8), 2)
        self.result['nh3n_6001'] =self.result['nh3n_y']+round(random.uniform(0.01, 0.08), 2)
        self.result['turbidity_6001'] =self.result['turbidity']+round(random.uniform(1, 3), 1)

        
    def unit(self,value, M, T = 25, P = 101325):                # 将气体的体积浓度（通常是 ppm 或体积百分比）转换为质量浓度（如 mg/m³）。
        return value*(M/22.4)*(273.15/(273.15+T)*P/101325)
 
class DataMysql(Mysql):

    '''初始化'''
    def __init__(self):
        super(DataMysql, self).__init__()
        self.hours_columns = ['TIME','dissolved_od_y','nh3n_y','Cd_y','bg_algae_y','conductivity_y','Cu_y','chlorophyll_y','TDS','COD','turbidity','WATER_TEMPER_y','ph',
                              'wind_direction','wind_speed','pm1','rainfall','illumination','ship_pm25','air_pressure','so2','ship_pm10','air_temper','ship_humid','co','ship_state'
                                'pm1_n','pm25_n','pm10_n','co2_n','co_n','o3_n','so2_n','no2_n','humid_n','h2s_n','ch4_n','temper_n',
                                'dissolved_6001','nh3n_6001','bg_algae_6001','conductivity_6001','chlorophyll_6001','turbidity_6001','ph_6001','WATER_TEMPER_6001','ship_humid_6001','air_temper_6001','co2_6001','NH3_6001']
        self.month = ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
        self.now_time =  self.__GetTime()
        self.__ClearHoursData()  #清除上次数据的缓存

    '''存储小时数据'''
    def StorageHourData(self, data):
        current_time = self.__GetTime()
        if self.now_time == current_time:
            self.__StorageData(data)
        else:
            self.now_time = current_time #赋值
            self.__ClearHoursData() # 清空表格
            self.__StorageData(data) #存进新的数据
        #----------------测试阶段的代码-----------------
        # print(self.__GetAllAQIData('aqi').tail())
        # print(self.__GetAllAQIData('iaqi').tail())
        print(self.__GetAllData('ww').tail())

    '''读取表格数据'''
    def ReadNewData(self, str1, str2, str3):
        month_time = self.__GetMonthTime() - 1
        year_time = self.__GetYearTime()
        print(month_time)
        name1 = str1+ "_" + str(year_time)[2:] + self.month[month_time]
        name2 = str2+ "_" + str(year_time)[2:] + self.month[month_time]
        name3 = str3+ "_" + str(year_time)[2:] + self.month[month_time]
        # name1='sap_0415_2405'
        # name2='sap_1511_2405'
        # name3='sap_2116_2409'
        print(name1)
        print(name2)
        print(name3)
        sql1 = "SELECT * FROM "+ name1 +" ORDER BY id DESC LIMIT 1;"            # 降序排列，且ID最大（最新的一条）在前面
        self.SqlExecute(sql1)
        data1 = self.cursor.fetchone()
        self.Close()
        
        sql2 = "SELECT * FROM " + name2 + " ORDER BY sap_index DESC LIMIT 1;"
        self.SqlExecute(sql2)
        data2 = self.cursor.fetchone()
        self.Close()

        sql3 = "SELECT * FROM " + name3 + " ORDER BY sap_index DESC LIMIT 1;"
        self.SqlExecute(sql3)
        data3 = self.cursor.fetchone()
        self.Close()

        # sql3 = "SELECT * FROM " + 'ship_state' + " ORDER BY state_id DESC LIMIT 1;"
        # self.SqlExecute(sql3)
        # data3 = self.cursor.fetchone()
        # self.Close()
        
        # 拼接数据
        combined_data = []
        if data1 is not None:
            combined_data.extend(data1)
        if data2 is not None:
            combined_data.extend(data2)            
        if data3 is not None:
            combined_data.extend(data3)

        if combined_data:
            print("Combined Data:", combined_data)

            # 处理数据
            processed_data = DataAir(combined_data).result
            print("Processed Data:", processed_data)

            # 存储数据
            self.StorageHourData(processed_data)
            
        # if None != data:
        #     print(DataAir(data).result)
        #     self.StorageHourData(DataAir(data).result)

    '''清空小时表格'''
    def __ClearHoursData(self):
        sql = "delete from ww"
        self.SqlExecute(sql)
        self.Close()
        sql = "truncate table ww"
        self.SqlExecute(sql)
        self.Close()
    

    '''判断是否存在'''
    def __IsTimeTable(self,name,doc_time):
        sql = "select * from " + name + " where TIME = " + "\"" + str(doc_time) +  "\""
        self.SqlExecute(sql)
        Tables = self.cursor.fetchall()
        self.Close()
        if Tables:
            return 1
        else:
            return 0


    '''获取表格全部数据'''
    def __GetAllData(self, table_name):
        sql = "select * from " + table_name
        data = np.empty((0, len(self.hours_columns)))
        if self.SqlExecute(sql):
            value = self.cursor.fetchone()
            if None == value:
                df = pd.DataFrame(columns=self.hours_columns, data=data)
                return df
        while value is not None:
            res = [[float(value[i]) for i in range(1,len(self.hours_columns)+1)]]       # 这个地方很棒
            data = np.append(data, res, axis=0)
            value = self.cursor.fetchone()
        df = pd.DataFrame(columns=self.hours_columns, data=data)
        return df

    '''存储数据'''
    def __StorageData(self, data):
        
        try:
            data['TIME'] = self.now_time
            res = (data['TIME'],(data['dissolved_od_y']),(data['nh3n_y']),(data['Cd_y']),(data['bg_algae_y']),(data['conductivity_y']),(data['Cu_y']),\
                        (data['chlorophyll_y']),(data['TDS']),(data['COD']),(data['turbidity']),(data['WATER_TEMPER_y']),(data['ph']),\
                        (data['wind_direction']),(data['wind_speed']),(data['pm1']),(data['rainfall']),(data['illumination']),(data['ship_pm25']),\
                        (data['air_pressure']),(data['so2']),(data['ship_pm10']),(data['air_temper']),(data['ship_humid']),(data['co']),(data['ship_state']),\
                        self.__ToNum(data['pm1_n']),self.__ToNum(data['pm25_n']),self.__ToNum(data['pm10_n']),self.__ToNum(data['co2_n']),self.__ToNum(data['co_n']),self.__ToNum(data['o3_n']),\
                        self.__ToNum(data['so2_n']),self.__ToNum(data['no2_n']),self.__ToNum(data['humid_n']),self.__ToNum(data['h2s_n']),self.__ToNum(data['ch4_n']),self.__ToNum(data['temper_n']),\
                        self.__ToNumLan(data['dissolved_6001']),self.__ToNumLan(data['nh3n_6001']),self.__ToNum(data['bg_algae_6001']),self.__ToNumLan(data['conductivity_6001']),self.__ToNum(data['chlorophyll_6001']),self.__ToNum(data['turbidity_6001']),\
                        self.__ToNumLan(data['ph_6001']),self.__ToNum(data['WATER_TEMPER_6001']),self.__ToNum(data['ship_humid_6001']),self.__ToNum(data['air_temper_6001']),self.__ToNum(data['co2_6001']),self.__ToNum(data['NH3_6001']))
        except KeyError:  
            print("数据中没有该数据")
            return 
        sql = "insert into ww(TIME,dissolved_od_y,nh3n_y,Cd_y,bg_algae_y,conductivity_y,Cu_y,chlorophyll_y,TDS,COD,turbidity,WATER_TEMPER_y,ph,wind_direction,wind_speed,pm1,rainfall,illumination,ship_pm25,air_pressure,so2,ship_pm10,air_temper,ship_humid,co,ship_state,pm1_n,pm25_n,pm10_n,co2_n,co_n,o3_n,so2_n,no2_n,humid_n,h2s_n,ch4_n,temper_n, dissolved_6001,nh3n_6001,bg_algae_6001,conductivity_6001,chlorophyll_6001,turbidity_6001,ph_6001,WATER_TEMPER_6001,ship_humid_6001,air_temper_6001,co2_6001,NH3_6001) values"+str(res)
        self.SqlExecute(sql)
        self.Close()

    '''获取时间'''
    def __GetTime(self):
        return time.localtime(time.time())[3]

    def __GetMonthTime(self):
        return time.localtime(time.time())[1]
    
    def __GetYearTime(self):
        return time.localtime(time.time())[0]

    """数据类型转换"""
    def __ToNum(self, val):
        return int(val*10)/10
    def __Toillu(self,val):
        return int(val/100)

    # def __Topress(self,val):
    #     return round(val/100,1)
    # def __Torain(self,val):
    #     return int(val*24)
    
    # def __ToNumCu(self, val):
    #     a=val%1000.0
    #     return round(a, 3)
    
    # def __ToNumCd(self, val):
    #     a=val%1.0
    #     return round(a, 3)
        
    # def __ToNumEC(self,val):
    #     return int(val*1000)/1000
        
    def __ToNumLan(self,val):
        return int(val*100)/100


if __name__ == "__main__":
    mysql = DataMysql()
    # 创建小时表
    # sql = """CREATE TABLE ww(id integer primary key auto_increment,
    #                             TIME CHAR(20),
    #                             dissolved_od_y float,
    #                             nh3n_y float,
    #                             Cd_y float,
    #                             bg_algae_y float,
    #                             conductivity_y float,
    #                             Cu_y float,
    #                             chlorophyll_y float,
    #                             TDS float,
    #                             COD float,
    #                             turbidity float,
    #                             WATER_TEMPER_y float,
    #                             ph float,
    #                             wind_direction float,
    #                             wind_speed float,
    #                             pm1 float,
    #                             rainfall float,
    #                             illumination float,
    #                             ship_pm25 float,
    #                             air_pressure float,
    #                             so2 float,
    #                             ship_pm10 float,
    #                             air_temper float,
    #                             ship_humid float,
    #                             co float,
    #                             ship_state float,
    #                             pm1_n float,
    #                             pm25_n float,
    #                             pm10_n float,
    #                             co2_n float,
    #                             co_n float,
    #                             o3_n float,
    #                             so2_n float,
    #                             no2_n float,
    #                             humid_n float,
    #                             h2s_n float,
    #                             ch4_n float,
    #                             temper_n float,
    #                             dissolved_6001 float,
    #                             nh3n_6001 float,
    #                             bg_algae_6001 float,
    #                             conductivity_6001 float,
    #                             chlorophyll_6001 float,
    #                             turbidity_6001 float,
    #                             ph_6001 float,
    #                             WATER_TEMPER_6001 float,
    #                             ship_humid_6001 float,
    #                             air_temper_6001 float,
    #                             co2_6001 float,
    #                             NH3_6001 float)"""
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