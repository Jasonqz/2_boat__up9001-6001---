// 更新时间
// (function () {
//     var t = null;
//     t = setTimeout(time, 1000);//開始运行
//     function time() {
//         clearTimeout(t);//清除定时器
//         dt = new Date();
//         var y = dt.getFullYear();
//         var mt = dt.getMonth() + 1;
//         var day = dt.getDate();
//         var h = dt.getHours();//获取时
//         var m = dt.getMinutes();//获取分
//         var s = dt.getSeconds();//获取秒
//         document.querySelector(".showtime1").innerHTML = '当前时间：' + y + "年" + mt + "月" + day + "日-" + h + "时" + m + "分" + s + "秒";
//         t = setTimeout(time, 1000); //设定定时器，循环运行     
//     }
// })();


(function () {
    var t1 = null;
    var t2 = null;
    var t3 = null;
    t1 = setTimeout(time1, 1000);
    t2 = setTimeout(time2, 1000);
    t3 = setTimeout(time3, 1000);
    t4 = setTimeout(time4, 1000);
    function time1() {
        clearTimeout(t1);
        var dt = new Date();
        var y = dt.getFullYear();
        var mt = dt.getMonth() + 1;
        var day = dt.getDate();
        var h = dt.getHours();
        var m = dt.getMinutes();
        var s = dt.getSeconds();
        document.querySelector("#water").innerHTML = '当前时间：' + y + "年" + mt + "月" + day + "日-" + h + "时" + m + "分" + s + "秒";
        t1 = setTimeout(time1, 1000);
    }

    function time2() {
        clearTimeout(t2);
        var dt = new Date();
        var y = dt.getFullYear();
        var mt = dt.getMonth() + 1;
        var day = dt.getDate();
        var h = dt.getHours();
        var m = dt.getMinutes();
        var s = dt.getSeconds();
        document.querySelector("#air").innerHTML = '当前时间：' + y + "年" + mt + "月" + day + "日-" + h + "时" + m + "分" + s + "秒";
        t2 = setTimeout(time2, 1000);
    }

    function time3() {
        clearTimeout(t3);
        var dt = new Date();
        var y = dt.getFullYear();
        var mt = dt.getMonth() + 1;
        var day = dt.getDate();
        var h = dt.getHours();
        var m = dt.getMinutes();
        var s = dt.getSeconds();
        document.querySelector("#boat").innerHTML = '当前时间：' + y + "年" + mt + "月" + day + "日-" + h + "时" + m + "分" + s + "秒";
        t3 = setTimeout(time3, 1000);
    }
    function time4() {
        clearTimeout(t4);
        var dt = new Date();
        var y = dt.getFullYear();
        var mt = dt.getMonth() + 1;
        var day = dt.getDate();
        var h = dt.getHours();
        var m = dt.getMinutes();
        var s = dt.getSeconds();
        document.querySelector("#boat6001").innerHTML = '当前时间：' + y + "年" + mt + "月" + day + "日-" + h + "时" + m + "分" + s + "秒";
        t4 = setTimeout(time4, 1000);
    }

})();


// 模块滑动动画
// (function () {
//     var imgs = ["static/images/img_2.png"]
//     var div1 = document.getElementById('protect3');
//     var len = imgs.length;
//     var qiehuan = 0;
//     var time_num = 60000;
//     function update_kepu() {
//         dt = new Date();
//         var m = dt.getMinutes();//获取分

//         if (qiehuan == 0 && m >= 4 && m != 59) {
//             // div1.style.display = 'block';
//             div1.style.visibility = 'visible';
//             qiehuan = 1;
//         } else if (qiehuan == 1 || m < 4 || m == 59) {
//             // div1.style.display = 'none';
//             div1.style.visibility = 'hidden';
//             //div1.style.backgroundImage="url("+imgs[i]+")";
//             qiehuan = 0;
//         }

//     }
//     window.setInterval(update_kepu, time_num);
// })();


(function () {
    var div2 = document.getElementById('protect1');
    var div3 = document.getElementById('protect2');
    var div31 = document.getElementById('protect22');
    var div4 = document.getElementById('protect3');
    var div5 = document.getElementById('protect4');
    var div6 = document.getElementById('protect31');
    var div7 = document.getElementById('protect41');
    var boss = document.getElementById('box');
    var boss1 = document.getElementById('box1');
    var qiehuan = 0;
    var time_num = 8000;
    var isPaused = false;
    var swapTime = 4000;
    //根据div可见状态改变盒子位置
    function checkAndSwap() {
        var bossComputedStyle = window.getComputedStyle(boss);
        var boss1ComputedStyle = window.getComputedStyle(boss1);
        if (div3.style.visibility === 'visible') {

            setTimeout(function () {
                if (div3.style.visibility === 'visible') {
                    boss.style.top = "220%";
                    boss1.style.top = "0";
                }
            }, swapTime);
        } else {
            // swapPositions();
            boss.style.top = "0";
            boss1.style.top = "220%";
        }
    }
    function update_kepu() {
        if (!isPaused) {
            if (qiehuan === 0) {
                div2.style.visibility = 'visible';
                div3.style.visibility = 'hidden';
                div4.style.visibility = 'hidden';
                div31.style.visibility = 'hidden';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'hidden';
                qiehuan = 1;
            } else if (qiehuan === 1) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'visible';
                div31.style.visibility = 'hidden';
                div4.style.visibility = 'hidden';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'hidden';
                qiehuan = 2;
            } else if (qiehuan === 2) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'hidden';
                div31.style.visibility = 'hidden';
                div4.style.visibility = 'hidden';
                div5.style.visibility = 'visible';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'hidden';
                qiehuan = 3;
            } else if (qiehuan === 3) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'hidden';
                div31.style.visibility = 'hidden';
                div4.style.visibility = 'hidden';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'visible';
                div7.style.visibility = 'hidden';
                qiehuan = 4;
            } else if (qiehuan === 4) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'hidden';
                div31.style.visibility = 'visible';
                div4.style.visibility = 'hidden';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'hidden';
                qiehuan = 5;
            } else if (qiehuan === 5) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'hidden';
                div31.style.visibility = 'hidden';
                div4.style.visibility = 'hidden';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'visible';
                qiehuan = 6;
            } else if (qiehuan === 6) {
                div2.style.visibility = 'hidden';
                div3.style.visibility = 'hidden';
                div31.style.visibility = 'hidden';
                div4.style.visibility = 'visible';
                div5.style.visibility = 'hidden';
                div6.style.visibility = 'hidden';
                div7.style.visibility = 'hidden';
                qiehuan = 0;
            }
            checkAndSwap();
        }
    }


    function togglePause() {
        isPaused = !isPaused;
    }

    // 添加空格键的按下事件监听器
    window.addEventListener('keydown', function (event) {
        if (event.code === 'Space') {
            togglePause();
        }
    });

    window.setInterval(update_kepu, time_num);
})();



// 更新主界面

// //
// (function(){

// })();