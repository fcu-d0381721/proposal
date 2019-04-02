from dbconnect import connectDB
from datetime import datetime, timedelta
import dateutil.relativedelta as relativedelta
import dateutil.rrule as rrule
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
import matplotlib.dates as mdate
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU
import pandas as pd
import numpy
from matplotlib.font_manager import FontProperties

config = []
twoWay = ["_rent", "_return"]
font = FontProperties(fname='/Library/Fonts/Arial Unicode.ttf',size=10)

# 有下雨的日期
rain = ['2017-06-02', '2017-06-03', '2017-06-04', '2017-06-05', '2017-06-10', '2017-06-11', '2017-06-12', '2017-06-13',
        '2017-06-14', '2017-06-15', '2017-06-16', '2017-06-17', '2017-06-18', '2017-06-19', '2017-06-21', '2017-06-30',
        '2017-07-01', '2017-07-03', '2017-07-04', '2017-07-07', '2017-07-08', '2017-07-09', '2017-07-10', '2017-07-11',
        '2017-07-23', '2017-07-28', '2017-07-29', '2017-07-31', '2017-08-02', '2017-08-03', '2017-08-04', '2017-08-11',
        '2017-08-19', '2017-08-21', '2017-08-22', '2017-08-23', '2017-08-24', '2017-08-27', '2017-08-31', '2017-09-01',
        '2017-09-02', '2017-09-03', '2017-09-04', '2017-09-08', '2017-09-09', '2017-09-12', '2017-09-13', '2017-09-14',
        '2017-10-03', '2017-10-04', '2017-10-06', '2017-10-07', '2017-10-08', '2017-10-10', '2017-10-11', '2017-10-12',
        '2017-10-13', '2017-10-14', '2017-10-15', '2017-10-17', '2017-10-18', '2017-10-19', '2017-10-20', '2017-10-21',
        '2017-10-24', '2017-10-27', '2017-10-28', '2017-10-29', '2017-10-30', '2017-10-31', '2017-11-01', '2017-11-01',
        '2017-11-03', '2017-11-04', '2017-11-06', '2017-11-09', '2017-11-11', '2017-11-12', '2017-11-13', '2017-11-14',
        '2017-11-18', '2017-11-19', '2017-11-20', '2017-11-21', '2017-11-22', '2017-11-23', '2017-11-24', '2017-11-25',
        '2017-11-26', '2017-11-27', '2017-12-01', '2017-12-02', '2017-12-03', '2017-12-05', '2017-12-06', '2017-12-07',
        '2017-12-08', '2017-12-09', '2017-12-12', '2017-12-13', '2017-12-16', '2017-12-17', '2017-12-18', '2017-12-19',
        '2017-12-28', '2017-12-29', '2017-12-30', '2017-12-31', '2018-01-04', '2018-01-05', '2018-01-06', '2018-01-07',
        '2018-01-08', '2018-01-09', '2018-01-19', '2018-01-20', '2018-01-22', '2018-01-28', '2018-01-29', '2018-01-30',
        '2018-01-31', '2018-02-01', '2018-02-02', '2018-02-03', '2018-02-04', '2018-02-05', '2018-02-07', '2018-02-08',
        '2018-02-09', '2018-02-10', '2018-02-11', '2018-02-21', '2018-02-22', '2018-02-25', '2018-02-26', '2018-02-28',
        '2018-03-04', '2018-03-05', '2018-03-07', '2018-03-08', '2018-03-14', '2018-03-15', '2018-03-16', '2018-03-20',
        '2018-03-21', '2018-03-27', '2018-04-06', '2018-04-07', '2018-04-12', '2018-04-14', '2018-04-15', '2018-04-16',
        '2018-04-17', '2018-04-24', '2018-04-30', '2018-05-03', '2018-05-08', '2018-05-29', '2018-05-30', '2018-06-01',
        '2018-06-05', '2018-06-06', '2018-06-10', '2018-06-11', '2018-06-14', '2018-06-15', '2018-06-18', '2018-06-21',
        '2018-06-22', '2018-06-23', '2018-06-29', '2018-06-30']


# 搜尋的星期
def search_week(year, month, start, end):
    # 設定開始日期跟結束日期
    before = datetime(year, month, start)
    after = datetime(year+1, month, end)
    # 可以選擇你要星期幾
    rr = rrule.rrule(rrule.WEEKLY, byweekday=(relativedelta.TU, relativedelta.WE, relativedelta.TH), dtstart=before)
    rr = rr.between(before, after, inc=True)
    return rr


# 有多少資料表名稱
def GetTableFromMySQL(x):
    global config
    table = x.query_table_for_show()
    for i in table:
        temp = i.get('Tables_in_YouBike')
        # 去除除了數字以外的字
        config.append(temp.strip('A_rentu'))
    # 在砍掉最後一個info
    config.pop(-1)
    config = list(set(config))
    print('------success query------')


# 把秒數轉時分秒
def GetTime(sec):
    d = datetime(1, 1, 1) + timedelta(seconds=sec)
    return d


# 讀取Mysql資料 再依有下雨、沒下雨、總資料做輸出
def readMysqlData(x, name, act, want_day):

    global rain
    pd.set_option('display.width', 200)
    pd.set_option('display.max_columns', 20)

    all_data = x.query_data_for_show("A" + name + act)
    all_data['r_time'] = all_data['r_time'].apply(lambda i: i.strftime('%Y-%m-%d'))
    all_data['r_time'] = [datetime.strptime(x, '%Y-%m-%d') for x in all_data['r_time']]
    s = []
    for i in all_data['r_time']:
        if i in want_day:
            s.append(i)
    all_data = all_data[all_data['r_time'].isin(s)].reset_index(drop=True)
    not_rain = all_data[~all_data['r_time'].isin(rain)].reset_index(drop=True)
    have_rain = all_data[all_data['r_time'].isin(rain)].reset_index(drop=True)
    return not_rain, have_rain, all_data


# 把資料裡面唯一時間取出來
def ToListTime(all_data):
    all_data['r_time'] = all_data['r_time'].apply(lambda i: i.strftime('%Y-%m-%d'))
    rent_time = all_data['r_time'].unique()
    rent_time = numpy.sort(rent_time, axis=0)
    all_time = rent_time.tolist()

    return all_time


# 把資料整理成 一天多少借量、租量、平均使用時間
def clearDataToShow(all_data, all_time):

    count = []
    average_time = []
    for item in all_time:
        total_time = 0
        time = datetime.strptime(item, '%Y-%m-%d')
        year = time.year
        month = time.month
        day = time.day
        all_data['r_time'] = pd.to_datetime(all_data['r_time'])
        con = all_data['r_time'].dt.year == year
        con1 = all_data['r_time'].dt.month == month
        con2 = all_data['r_time'].dt.day == day
        result = all_data[con & con1 & con2]
        temp = pd.Series(result['t_time'].get_values())
        temp = temp.str.split(':')

        for k in temp:
            try:
                hour = int(k[0]) * 60 * 60
                minute = int(k[1]) * 60
                second = int(k[2])
            except:
                hour = 0
                minute = 0
                second = 0
            total_time += hour + minute + second
        total_time = total_time / len(result)
        total_time = round(total_time, 0)
        total_time = GetTime(total_time)
        f = str(total_time).split(" ")
        average_time.append(f[1])
        count.append(len(result))

    return count, average_time


# 將總時間跟區別區來得借量、租量做比較補值
def expand_list(full_list, short_list, count):
    t = []
    cnt = 0
    for x in full_list:
        if x in short_list:
            t.append(count[cnt])
            cnt += 1
        else:
            t.append(0)
    return t


# 同上 不過是讓平均時間補值
def expand_list_for_time(full_list, short_list, count):
    t = []
    cnt = 0
    for x in full_list:

        if x in short_list:
            t.append(count[cnt])
            cnt += 1
        else:
            t.append('00:00:01')
    return t


# 作圖的地方
def showPicture(i, name, rent_no_rain_count, return_no_rain_count, rent_rain_count, return_rain_count, rent_time,
                return_time, count):
    # 中文字體的顯示
    global font
    register_matplotlib_converters()

    rent_time = [datetime.strptime(str(x), '%H:%M:%S') for x in rent_time]
    return_time = [datetime.strptime(str(x), '%H:%M:%S') for x in return_time]
    count = [datetime.strptime(str(x), '%Y-%m-%d') for x in count]

    fig = plt.figure(figsize=(20, 8))
    ax1 = fig.add_subplot(1, 1, 1)

    # 租借量有下雨無下雨長條圖
    plt.bar(count, rent_no_rain_count, alpha=0.5, color='#4efeb3', align="edge", width=0.5, label="rent_no_rain_count")
    plt.legend(loc=2)
    plt.bar(count, return_no_rain_count, alpha=0.5, color='#02df82', align="edge", width=-0.5, label="return_no_rain_count")
    plt.legend(loc=2)
    plt.bar(count, rent_rain_count, alpha=0.5, color='#84c1ff', align="edge", width=0.5, label="rent_rain_count")
    plt.legend(loc=2)
    plt.bar(count, return_rain_count, alpha=0.5, color='#0072e3', align="edge", width=-0.5, label="return_rain_count")
    plt.legend(loc=2)

    # 共用x軸
    ax2 = ax1.twinx()

    # x, y軸刻度設為時間、範圍
    ax2.yaxis.set_major_formatter(mdate.DateFormatter('%H:%M:%S'))
    ax2.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d'))
    ax2.set_ylim(datetime(1900, 1, 1, 0, 0), datetime(1900, 1, 1, 1, 0, 0))
    ax2.set_xlim(datetime(2017, 6, 1, 0, 0), datetime(2018, 6, 30, 23, 59, 59))
    # x軸只有選取之星期
    ax1.xaxis.set_major_locator(mdate.WeekdayLocator(byweekday=(TU, WE, TH)))

    # 平均時間折線圖
    plt.plot(count, rent_time, alpha=0.5, color='#b15bff', label="rent_time")
    plt.legend(loc=1)
    plt.plot(count, return_time, alpha=0.5, color='#ff0000', label="return_time")
    plt.legend(loc=1)

    plt.title(name, fontproperties=font, fontsize=20)
    # 讓你的座標軸不要過於密集
    # 先全部隱藏起來
    for label in ax1.get_xticklabels():
        label.set_visible(False)
        # label.set_rotation(45)
    # 在三個三個顯示
    for label in ax1.get_xticklabels()[::3]:
        label.set_visible(True)
        label.set_rotation(45)
    # 輸出圖片檔
    plt.savefig("./TUEtoTHU/" + name + ".png")
    print('------success print ' + i + " " + name + '.png------')
    # 關掉python輸出視窗
    plt.clf()
    plt.close()
    # plt.show()


# 主程式
def main():

    # 連接資料庫
    x = connectDB()
    # 選取想要的星期之範圍
    want_day = search_week(2017, 6, 1, 30)

    GetTableFromMySQL(x)
    for i in config:
        combine = []  # 總時間
        count = []  # 租量、借量之總數
        time = []  # 租量、借量之時間總數
        for j in twoWay:
            data = readMysqlData(x, i, j, want_day)
            # data[0]  # not_rain
            # data[1]  # is_rain
            # data[2]  # all_data
            all_day = ToListTime(data[2])
            combine.append(all_day)
            q = clearDataToShow(data[2], all_day)
            time.append(q[1])
            for k in range(2):
                get = ToListTime(data[k])
                combine.append(get)
                # rent 沒雨有雨[1][2]
                # return 沒雨有雨[4][5]
                get1 = clearDataToShow(data[k], get)
                count.append(get1[0])

        # rent count [0][1] 沒下雨 有下雨
        # return count [2][3] 沒下雨 有下雨

        all_time = list(set(combine[0] + combine[3]))  # 全部資料
        all_time.sort()

        rent_no_rain_count = expand_list(all_time, combine[1], count[0])
        return_no_rain_count = expand_list(all_time, combine[4], count[2])

        rent_rain_count = expand_list(all_time, combine[2], count[1])
        return_rain_count = expand_list(all_time, combine[5], count[3])

        rent_time = expand_list_for_time(all_time, combine[0], time[0])
        return_time = expand_list_for_time(all_time, combine[3], time[1])

        name = x.query_data_name("A" + i)
        name = name[0].get('sna')
        showPicture(i, name, rent_no_rain_count, return_no_rain_count, rent_rain_count, return_rain_count,
                    rent_time, return_time, all_time)
    x.exit()


if __name__ == "__main__":
    main()
