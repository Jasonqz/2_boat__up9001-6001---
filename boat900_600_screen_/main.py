'''代码是一个Flask Web 服务器'''
import json
from flask import Flask, render_template, request
# 介绍一下Flask，Flask是用Python 编写的轻量级 Web 应用框架。它的设计哲学强调简单性和易于使用的 API。
from MainBlock import MainBlock, Visualization    # hh主要的显示模块
from manager import Manager
from data_mysql import DataMysql
from flask import jsonify
from gevent import pywsgi
app = Flask(__name__, template_folder="templates")   # Flask 会从 template/目录加载HTML文件
# app = Flask（） 作用：实例化Flask类
app.jinja_env.variable_start_string = '[['
app.jinja_env.variable_end_string = ']]'
# jinja 是Python的一个模板引擎，用于在HTML文件中嵌入动态内容

# 全局变量
class_list = [MainBlock(), Visualization()]     # 两个自定义类
manager = Manager(class_list=class_list,
                  data_mysql=DataMysql())         # 此处用于数据库交互，此处有疑问

# 更新传感器参数,没有这个压根没数据
@app.route('/update', methods=['POST'])
def update():
    result = {}
    if request.method == 'POST':
        data = eval(request.get_data())               # 将数据解析成Python字典
        result = manager.GetJson(int(data['flag']))   # 调用接口
    return result

# 获取前端页面
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')    # Flask 会从加载template_folder目录

if __name__== '__main__':

    server = pywsgi.WSGIServer(('127.0.0.1', 5008), app)
    server.serve_forever()

