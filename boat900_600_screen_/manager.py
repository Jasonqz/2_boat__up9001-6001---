import pandas as pd
import numpy as np
from data_mysql import DataMysql
import time
from gevent import pywsgi
from mysql import Mysql


class Manager:
    '''初始化'''

    def __init__(self, class_list=[], data_mysql=None):
        super(Manager, self).__init__()

        # 1.初始化变量
        self.mysql = data_mysql  # 数据库管理，data_mysql是DataMysql()的实例
        self.class_list = class_list  # class_list 里面存放了两个自定义类

    '''前端交互'''

    def GetJson(self, flag):
        if 0 == flag:  # 全部数据
            return self.__GetAllJson()

    """all数据"""

    def __GetAllJson(self):
        res = {}
        print(res)
        data2 = self.mysql.FindMaxData()
        print("所有数据：", data2)
        res['sensor'] = (data2[0:12])
        res['sensor1'] = (data2[12:24])
        res['state'] = (data2[24:25])
        res['sensor2'] = (data2[25:37])
        res['sensor3'] = (data2[37:49])

        print("*" * 25 + "传感器数据" + "*" * 25)
        print("水质数据(9001)", res['sensor'])
        print("气象数据(9001)", res['sensor1'])
        print("船状态(9001)", res['state'])
        print("逸夫楼", res['sensor2'])
        print("6001", res['sensor3'])
        return res


if __name__ == '__main__':
    pass
