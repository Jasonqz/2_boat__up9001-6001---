# Vue 一周培训内容（基于"水环境在线监测大屏"项目）

## 培训大纲总览

| 天数 | 主题 | 关键词 |
| --- | --- | --- |
| Day 1 | Vue 入门与环境搭建 | MVVM、CDN 引入、第一个实例、与 Flask 模板兼容 |
| Day 2 | 模板语法与指令 | 插值、v-bind、v-on、v-if/v-else-if/v-else、v-for、v-model、修饰符 |
| Day 3 | 实例、data、methods、watch 与生命周期 | 选项、this 指向、watch 深度监听、created/beforeDestroy |
| Day 4 | axios 异步请求 + EventBus 跨实例通信 | 定时轮询、事件总线、与 Flask 后端联调 |
| Day 5 | 综合实战 + ECharts 整合 | 大屏搭建、动态样式、ECharts、项目复盘 |

---

## Day 1 — Vue 入门与环境搭建

### 1.1 什么是 Vue？为什么用 Vue？

**Vue 是一套用于构建用户界面的渐进式框架。** 核心特点：

- **响应式数据绑定**：数据变了，页面自动更新。
- **组件化开发**：界面拆成可复用的组件。
- **轻量、易上手**：可逐步引入，不必一次性全盘改造。

本项目用 Vue 的目的：把传感器数据通过 axios 拉回来后，**自动渲染到大屏**，避免手动操作 DOM。

### 1.2 MVVM 模型（理解即可）

```
View（页面）  <-->  ViewModel（Vue 实例）  <-->  Model（数据）
   DOM                data + methods                后端返回的 JSON
```

- View：HTML 模板
- Model：data 里的数据
- ViewModel：`new Vue({...})` 创建的实例，连接两者

### 1.3 环境搭建（CDN 方式，与项目一致）

项目采用的是 **CDN 引入 vue.min.js** 的方式，不需要 npm、webpack，适合快速嵌入到 Flask 渲染的 HTML 中。

参考项目 [templates/index.html](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L10)：

```html
<head>
    <script src="[[ url_for('static', filename='./js/vue.min.js') ]]"></script>
    <script src="[[ url_for('static', filename='./js/axios.min.js') ]]"></script>
    <script src="[[ url_for('static', filename='./js/echarts.min.js') ]]"></script>
</head>
```

> 说明：`[[ ... ]]` 是 Flask Jinja2 修改后的定界符，因为 Vue 默认的 `{{ }}` 会与 Jinja2 冲突。本项目在 [main.py](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/main.py#L12-L13) 中做了如下设置：

```python
app.jinja_env.variable_start_string = '[['
app.jinja_env.variable_end_string = ']]'
```

### 1.4 第一个 Vue 实例（课堂演示）

新建 `demo1.html`：

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <script src="https://cdn.jsdelivr.net/npm/vue@2.7.16/dist/vue.min.js"></script>
</head>
<body>
    <div id="app">
        <h1>{{ message }}</h1>
        <button v-on:click="change">点击改变</button>
    </div>
    <script>
        new Vue({
            el: '#app',
            data: {
                message: 'Hello Vue!'
            },
            methods: {
                change() {
                    this.message = '数据变了，页面自动更新'
                }
            }
        })
    </script>
</body>
</html>
```

### 1.5 实操练习（Day 1）

1. 在项目根目录新建 `demo/demo1.html`，本地用浏览器打开运行。
2. 把 `message` 改成一个数组 `['a','b','c']`，观察页面显示。
3. **思考题**：为什么项目用 CDN 而不是 NPM？（提示：与 Flask 模板渲染、部署方式相关）

### 1.6 当日小结

- Vue 通过 `new Vue({el, data, methods})` 启动
- 数据变化自动反映到视图
- CDN + Flask 模板要避免 `{{ }}` 冲突

---

## Day 2 — 模板语法与指令

### 2.1 文本插值

```html
<h3>实际值：{{ sensor_data[j*3 + i] }}{{ unit2[j*3 + i] }}</h3>
```

`{{ }}` 内部可以是**任意 JavaScript 表达式**：变量、运算、三元、函数调用等。

### 2.2 v-bind：绑定属性

```html
<div v-bind:id="'item-' + index"></div>
<!-- 简写 -->
<div :id="'item-' + index"></div>
```

### 2.3 v-on：监听事件

```html
<button v-on:click="GetJson('0')">刷新</button>
<!-- 简写 -->
<button @click="GetJson('0')">刷新</button>
```

### 2.4 v-if / v-else-if / v-else：条件渲染

这是本项目最密集使用的指令。参考 [templates/index.html#L562-L573](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L562-L573)：

```html
<h2 v-if="j*3+i == 0"> PM1<span>（超细颗粒物）</span></h2>
<h2 v-else-if="j*3+i == 1">PM2.5<span>（细颗粒物）</span></h2>
<h2 v-else-if="j*3+i == 2"> PM10<span>（可吸入颗粒物）</span></h2>
...
```

**项目特色用法**：根据数值区间显示不同颜色，参考 [index.html#L577-L613](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L577-L613)：

```html
<h3 v-if="sensor_data[j*3+i] >= 0 && sensor_data[j*3+i] < 25"
    style="color:rgba(0, 190, 0, 0.7)">
    实际值：{{ sensor_data[j*3+i] }}{{ unit2[j*3+i] }}<sup>3</sup>
</h3>
<h3 v-else-if="sensor_data[j*3+i] >= 25 && sensor_data[j*3+i] < 1000"
    style="color:rgba(255, 0, 0, 0.7)">
    实际值：{{ sensor_data[j*3+i] }}{{ unit2[j*3+i] }}<sup>3</sup>
</h3>
<h3 v-else>实际值：{{ sensor_data[j*3+i] }}{{ unit2[j*3+i] }}<sup>3</sup></h3>
```

### 2.5 v-for：列表渲染

项目大量使用 v-for 渲染传感器数据列表，参考 [index.html#L62-L77](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L62-L77)：

```html
<li v-for="(item, index) in sensor_data" :key="index">
    <h2>{{ objList[index].symbol }}<span>{{ objList[index].name }}</span></h2>
    <h3>实际值：{{ item }}{{ unit[index] }}</h3>
    <h4>标准值：{{ std[index] }}{{ unit[index] }}</h4>
</li>
```

**关键点**：
- `v-for` 必须加 `:key`（用 index 或唯一 id）
- `(item, index) in array` 是固定写法
- 多个数组按相同 index 对齐取值（项目里 `objList`、`unit`、`std` 与 `sensor_data` 一一对应）

### 2.6 v-show 与 v-if 的区别

- `v-if`：DOM 真实增删（条件为 false 时元素不存在）
- `v-show`：通过 `display:none` 控制（DOM 一直存在）
- 频繁切换用 `v-show`，一次性条件用 `v-if`

### 2.7 v-model：双向绑定（项目未直接使用，但需掌握）

```html
<input v-model="message">
<p>{{ message }}</p>
```

### 2.8 修饰符

- `.prevent`：阻止默认行为 `@click.prevent`
- `.stop`：阻止冒泡 `@click.stop`
- `.enter`：回车触发 `@keyup.enter="submit"`
- `.number`：转数字 `v-model.number`

### 2.9 实操练习（Day 2）

参考 [index.html](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html) 写一个 demo：
1. 一个数组 `students: [{name, score}]`，用 `v-for` 渲染成表格
2. 用 `v-if/v-else-if` 根据 score 显示等级（优/良/中/差）
3. 根据 score 区间给每行设置不同背景色

### 2.10 当日小结

- 指令是 `v-` 开头的特殊属性
- 项目里 `v-for + v-if` 是渲染动态数据面板的核心套路

---

## Day 3 — 实例、data、methods、watch 与生命周期

### 3.1 Vue 实例选项一览

参考项目的实例定义 [index.html#L852-L906](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L852-L906)：

```javascript
var vm = new Vue({
    el: '#mainbox',
    data: { /* 数据 */ },
    watch: { /* 监听 */ },
    methods: { /* 方法 */ },
    created() { /* 生命周期 */ },
    beforeDestroy() { /* 销毁前清理 */ }
});
```

### 3.2 data：响应式数据

项目中的 data 设计（节选）：

```javascript
data: {
    arr11: [0, 1, 2],
    arr22: [0, 1, 2, 3],
    objList: [
        { symbol: 'DO', name: '（溶解氧）' },
        { symbol: 'NH3-N', name: '（氨氮）' },
        // ...
    ],
    std: [">6", "<0.5", ...],
    unit: ["mg/L", "mg/L", ...],
    sensor_data: [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
    state_data: [-1],
    dict: { 'co': "主要污染物（一氧化碳）", ... }
}
```

**经验**：
- 多个并列数组用相同 index 取值是项目常用模式（虽然不算优雅，但简单直接）
- 初始值要给"占位值"（如 `-1`），避免模板渲染时报错

### 3.3 methods：方法定义

项目 [index.html#L930-L1236](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L930-L1236) 定义了大量方法：

```javascript
methods: {
    levelColor() { /* 根据数值改 DOM 颜色 */ },
    InitHtml() { /* 初始化拉数据 */ },
    GetJson(flag) { /* axios 请求 */ },
    StartTask(flag, data) { /* 分发任务 */ },
    InitTasks(data) { /* 数据回填 data */ },
    UpdateSensor(data) { /* 更新传感器 */ },
    gernerateId(index) { return index; }
}
```

### 3.4 this 指向问题（重点）

- 在 methods 内 `this` 指向 vm 实例
- **但在 axios 回调里 this 会丢失**！项目里用了经典的处理方式：

```javascript
GetJson: function(flag) {
    let that = this;  // 关键：保存 this
    axios.post("/update", { flag }).then(function(response) {
        that.StartTask(flag, response.data);  // 用 that
    });
}
```

也可以用箭头函数自动绑定外层 this：

```javascript
axios.post("/update", { flag }).then((response) => {
    this.StartTask(flag, response.data);
});
```

### 3.5 watch：监听数据变化

项目使用 watch 深度监听数组变化，参考 [index.html#L907-L929](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L907-L929)：

```javascript
watch: {
    sensor_data: {
        deep: true,           // 深度监听数组内部变化
        immediate: true,      // 初始化时立即执行一次
        handle() {
            this.levelColor()
        }
    },
    sensor_data1: {
        deep: true,
        immediate: true,
        handle() {
            this.levelColor1()
        }
    }
}
```

> 注意：项目里把 handler 写成了 `handle`，这是个**小笔误**，Vue 标准是 `handler`。实际运行时会按普通方法调用，效果一样。培训时要指出这一点。

### 3.6 生命周期钩子

项目用到的两个生命周期：

```javascript
created() {
    EventBus.$on('requestUpdate', this.GetJson);  // 实例创建后注册事件
},
beforeDestroy() {
    EventBus.$off('requestUpdate', this.GetJson);  // 销毁前解绑事件，避免内存泄漏
    clearInterval(this.intervalId);                 // 清理定时器
}
```

**生命周期流程**（重点记）：

```
created        -> 实例创建完成，data 已就绪，可访问 this
mounted        -> DOM 挂载完成，可访问 this.$el
beforeDestroy  -> 实例销毁前，清理定时器/事件监听
```

### 3.7 $refs：操作 DOM（谨慎使用）

项目在 methods 中通过 `$refs` 直接操作 DOM 改颜色，参考 [index.html#L932-L934](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L932-L934)：

```javascript
levelColor() {
    const h3s = this.$refs.level     // 模板里 ref="level" 的所有元素
    const h2s = this.$refs.dealsub
    const h4s = this.$refs.zxc
    for (let i = 0; i < this.objList.length; i++) {
        if (this.sensor_data[i] >= 6) h3s[i].style.color = 'rgba(0, 190, 0, 0.7)'
        // ...
    }
}
```

模板中对应：

```html
<h3 ref="level">实际值：{{ item }}{{ unit[index] }}</h3>
```

**原则**：能用 `:style`、`:class` 绑定的就别用 `$refs`，但批量操作时 `$refs` 更直接。

### 3.8 实操练习（Day 3）

1. 复刻项目 `levelColor` 的核心逻辑：定义 `values: [3, 7, 25]`，渲染成列表，根据数值给每行 `<li>` 设置不同文字颜色（用 `:style` 绑定）。
2. 把上面改成 `watch + $refs` 方式实现一遍，对比两种方式。
3. 在 `created` 里打印 "实例创建"，在 `beforeDestroy` 里清理一个定时器。

### 3.9 当日小结

- 实例选项：data、methods、watch、created、beforeDestroy
- axios 回调里 this 会丢失，需用 `that` 或箭头函数
- `$refs` 用于直接操作 DOM

---

## Day 4 — axios 异步请求 + EventBus 跨实例通信

### 4.1 axios 基础

axios 是基于 Promise 的 HTTP 客户端，用于向后端发请求。

```javascript
axios.post('/update', { flag: '0' })
     .then(function(response) { console.log(response.data) })
     .catch(function(err) { console.log(err) });
```

### 4.2 项目后端接口（必读）

Flask 后端定义在 [main.py](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/main.py#L22-L33)：

```python
@app.route('/update', methods=['POST'])
def update():
    result = {}
    if request.method == 'POST':
        data = eval(request.get_data())      # 解析前端 POST 数据
        result = manager.GetJson(int(data['flag']))
    return result
```

**接口约定**：
- URL：`POST /update`
- 请求体：`{ flag: '0' }`（字符串）
- 响应：JSON `{ sensor: [...], sensor1: [...], state: [...], sensor2: [...], sensor3: [...] }`

后端数据来源参考 [manager.py#L27-L44](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/manager.py#L27-L44)，把数据库查询结果切分成多段：

```python
res['sensor']  = data2[0:12]     # 9001船水质
res['sensor1'] = data2[12:24]    # 9001船气象
res['state']   = data2[24:25]    # 9001船状态
res['sensor2'] = data2[25:37]    # 6001船水质
res['sensor3'] = data2[37:49]    # 6001船气象
```

### 4.3 项目 axios 封装

项目把请求封装成 `GetJson` 方法，参考 [index.html#L1181-L1196](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L1181-L1196)：

```javascript
GetJson: function(flag) {
    let that = this;
    let data_json = {};
    data_json['flag'] = flag;
    axios.post("/update", data_json).then(function(response) {
        that.StartTask(flag, response.data);
        EventBus.$emit('updateData', response.data);  // 通过事件总线广播
    }).catch(function(err) {
        console.log(err);
    });
},
StartTask: function(flag, data) {
    if (flag == "0") this.InitTasks(data);
    else if (flag == "1") this.UpdateSensor(data);
    // ...
    this.levelColor();
    this.levelColor1();
}
```

**设计模式**：
- 一个 `GetJson` 入口负责请求
- 一个 `StartTask` 分发器根据 flag 决定调用哪个处理函数
- 数据到了通过 EventBus 广播给其他实例

### 4.4 定时轮询（项目核心）

项目用 `setInterval` 每 6 秒拉一次数据，参考 [index.html#L1250](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L1250)：

```javascript
window.setInterval(vm.GetJson, 6000, "0");  // 每6秒传 flag="0"
```

**注意**：`setInterval` 的参数透传写法（第三个参数起会作为回调的参数）。

### 4.5 EventBus：跨实例通信（项目亮点）

项目里页面有 3 个 Vue 实例（vm、vm1、vm2），分别管理 9001 船、大气、6001 船区域。它们需要共享同一份数据。项目用 EventBus 解决：

**1. 创建总线**（[index.html#L851](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L851)）：

```javascript
const EventBus = new Vue();   // 一个空的 Vue 实例当作事件总线
```

**2. 发送方（vm）**：拉到数据后广播

```javascript
EventBus.$emit('updateData', response.data);
```

**3. 接收方（vm1、vm2）**：在 `created` 里监听

```javascript
created() {
    EventBus.$on('updateData', this.InitTasks);
}
```

**4. 解绑**（避免内存泄漏）：

```javascript
beforeDestroy() {
    EventBus.$off('requestUpdate', this.GetJson);
}
```

**EventBus API**：
- `$on(事件名, 回调)` 监听
- `$emit(事件名, 数据)` 触发
- `$off(事件名, 回调)` 解绑

### 4.6 多实例协作全景图

```
              [Flask /update]
                    |
           axios.post (vm.GetJson)
                    |
        ┌───────────┴───────────┐
        ▼                       ▼
  vm.InitTasks(data)    EventBus.$emit('updateData', data)
  (9001船区域)              |
                       ┌────┴────┐
                       ▼         ▼
                  vm1.InitTasks  vm2.InitTasks
                  (大气)         (6001船)
```

### 4.7 实操练习（Day 4）

1. 启动项目（`python main.py`），访问 http://127.0.0.1:5008/ ，F12 打开控制台观察 `console.log` 输出的数据流。
2. 用 Postman 或 curl 向 `http://127.0.0.1:5008/update` 发 `{"flag":"0"}` 请求，观察返回结构。
3. 写一个 demo：两个 Vue 实例 A、B，A 里有个输入框，B 里显示 A 输入的内容，用 EventBus 实现。

### 4.8 当日小结

- axios 发请求 → 后端返回 JSON → 前端分发
- 定时轮询用 `setInterval`
- 多实例通信用 EventBus（`$on` / `$emit` / `$off` 三件套）

---

## Day 5 — 综合实战 + ECharts 整合

### 5.1 ECharts 基础

ECharts 是开源的可视化库，项目用它做 AQI 图表。

```javascript
var chart = echarts.init(document.getElementById('chart'));
chart.setOption({
    title: { text: 'AQI 趋势' },
    xAxis: { type: 'category', data: ['0','1','2','3'] },
    yAxis: { type: 'value' },
    series: [{ data: [50, 80, 120, 60], type: 'line' }]
});
```

### 5.2 项目中 ECharts 与 Vue 的配合

项目里 ECharts 的 DOM 容器是固定的（如 `<div id="kepu1">`），由独立的 JS 负责初始化和更新，**Vue 不直接管理 ECharts 实例**，而是通过 EventBus 触发更新。这是一种解耦方式。

**项目模式**：
- Vue 负责 `data` 数据存储和文字渲染
- ECharts 实例放在外层闭包里，监听 EventBus 事件后用 `chart.setOption` 更新

### 5.3 flv.js 视频流（了解即可）

项目用 flv.js 播放船上摄像头的 RTSP 转 FLV 直播流，参考 [index.html#L164-L272](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/templates/index.html#L164-L272)：

```javascript
player = flvjs.createPlayer({
    type: 'flv',
    isLive: true,
    url: 'http://120.78.234.51:8080/live/34020000001320000002.flv'
});
player.attachMediaElement(videoDom);
player.load();
player.play();
```

**容错机制**：
- 监听 `flvjs.Events.ERROR`，出错后销毁重建
- 监听 `statistics_info`，缓冲超过 60 秒自动重启
- 视频不可用时显示替代图片

### 5.4 综合实战：仿写一个"机房温度监控大屏"

**需求**：
- 3 个区域：机房 A、机房 B、汇总
- 每 5 秒从后端拉一次数据
- 每个机房显示 6 个温度传感器（数字 + 颜色等级）
- 用 ECharts 画一条温度趋势折线图
- 用 EventBus 让汇总区域同步更新

**后端模板**（基于本项目简化）：

```python
# fake_server.py
from flask import Flask, jsonify, render_template
import random
app = Flask(__name__, template_folder='templates')

@app.route('/')
def index(): return render_template('demo.html')

@app.route('/update', methods=['POST'])
def update():
    return jsonify({
        'roomA': [round(random.uniform(20, 35), 1) for _ in range(6)],
        'roomB': [round(random.uniform(20, 35), 1) for _ in range(6)],
    })

if __name__ == '__main__':
    app.run(port=5009, debug=True)
```

**前端骨架**（学员补全）：

```html
<div id="roomA">
    <li v-for="(t, i) in temps" :key="i">
        传感器{{ i+1 }}: {{ t }}℃
        <span :style="{color: t < 25 ? 'green' : t < 30 ? 'orange' : 'red'}">
            {{ t < 25 ? '正常' : t < 30 ? '预警' : '报警' }}
        </span>
    </li>
</div>
<div id="roomB"><!-- 同上 --></div>
<div id="summary">
    <div id="chart" style="width:600px;height:400px"></div>
</div>
```

```javascript
const EventBus = new Vue();

const vmA = new Vue({
    el: '#roomA',
    data: { temps: Array(6).fill(0) },
    created() { EventBus.$on('updateData', d => this.temps = d.roomA) }
});
const vmB = new Vue({
    el: '#roomB',
    data: { temps: Array(6).fill(0) },
    created() { EventBus.$on('updateData', d => this.temps = d.roomB) }
});

const chart = echarts.init(document.getElementById('chart'));

function poll() {
    axios.post('/update').then(res => {
        EventBus.$emit('updateData', res.data);
        // 更新 ECharts
        chart.setOption({
            xAxis: { type: 'category', data: ['A1','A2','A3','A4','A5','A6','B1','B2','B3','B4','B5','B6'] },
            yAxis: { type: 'value' },
            series: [{ data: [...res.data.roomA, ...res.data.roomB], type: 'bar' }]
        });
    });
}
poll();
setInterval(poll, 5000);
```


### 5.6 进阶建议（培训结束后）

学完本课程后，可以继续深入：
1. **Vue 3 + Composition API**：更现代的写法，更易复用逻辑
2. **工程化（Vite + Vue 单文件组件）**：替代 CDN 方式
3. **Pinia**：替代 EventBus 做状态管理
4. **组件化拆分**：把当前一个大页面拆成多个 `.vue` 组件

---



## 附录 B：常见坑位清单

1. **Jinja2 与 Vue 的 `{{ }}` 冲突** → 改 Flask 定界符为 `[[ ]]`（见 [main.py#L12](file:///c:/Users/28478/Desktop/2_boat__up9001+6001%20-%20实验室大屏/boat900_600_screen_/main.py#L12)）
2. **axios 回调里 this 丢失** → 用 `let that = this` 或箭头函数
3. **v-for 没加 key** → 始终加 `:key`
4. **watch 数组变化没触发** → 加 `deep: true`
5. **EventBus 事件没解绑** → 一定要在 `beforeDestroy` 里 `$off`
6. **setInterval 没清理** → `beforeDestroy` 里 `clearInterval`
7. **后端 `eval(request.get_data())` 有安全风险** → 生产环境应改用 `json.loads`

## 附录 C：参考资源

- Vue 2 官方文档（中文）：https://v2.cn.vuejs.org/
- ECharts 官方文档：https://echarts.apache.org/zh/index.html
- axios 文档：https://axios-http.com/zh/
- Flask 中文文档：https://dormousehole.readthedocs.io/
