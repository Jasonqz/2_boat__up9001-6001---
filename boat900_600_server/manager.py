import pandas as pd
import numpy as np
import threading
from data_mysql import DataMysql, DataAir
import time
from mysql import Mysql


class Manager():

    '''初始化'''
    def __init__(self, data_mysql=None):
        super(Manager, self).__init__()

        # 1.初始化变量
        self.mysql = data_mysql   #数据库管理

    '''开炮'''
    def run(self):
        while(1):
            self.mysql.ReadNewData('yunyang', 'sap_1511', 'sap_2116')
            time.sleep(60)


if __name__== '__main__':
    manager = Manager(DataMysql())
    manager.run()