from dbconnect import connectDB
import pandas as pd
from datetime import datetime
import numpy as np

config = []
twoWay = ["_rent"]


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


# 讀取Mysql資料
def readMysqlData(x, name, act):

    all_data = x.query_data_for_show("A" + name + act)
    all_data['r_time'] = all_data['r_time'].apply(lambda i: i.strftime('%Y-%m-%d'))
    all_data['r_time'] = [datetime.strptime(x, '%Y-%m-%d') for x in all_data['r_time']]
    con1 = all_data["r_time"] <= datetime(2018, 6, 30)
    con2 = all_data["r_time"] >= datetime(2017, 6, 1)
    all_data = all_data[(con1 & con2)]

    return all_data


def clearUpData(data):

    data['r_time'] = pd.to_datetime(data['r_time'])
    # 切換 星期(freq='W-SUN') 月份(freq='M')
    group_new_data = data.groupby(pd.Grouper(key='r_time', freq='W-SUN'))["r_time"].count()
    group_new_data = pd.DataFrame({'count': group_new_data})
    group_new_data.reset_index(inplace=True)

    return group_new_data


def count_each_week(group_new_data):

    number = 0
    flag = 0
    for i in range(len(group_new_data)):
        if flag == 1:
            num = group_new_data['count'][i]-group_new_data['count'][i-1]
            number += np.square(num)
        else:
            flag = 1
    return np.sqrt(number)


# 主程式
def main():

    # 連接資料庫
    x = connectDB()
    GetTableFromMySQL(x)
    all_count = []
    percentile = []
    for i in config:
        for j in twoWay:
            data = readMysqlData(x, i, j)
            # avg_month = len(data)/13
            group_new_data = clearUpData(data)
            # print(len(group_new_data))
            percentile.append(len(group_new_data))
            all_count.append(group_new_data)
            print(i, percentile, all_count)
            # each_count.append(count_each_week(group_new_data))
            # each_count.append(avg_month)
        # all_count.append(each_count)
    # df_each_site = pd.DataFrame(all_count, columns=['name', 'rent', 'return'])
    # print(df_each_site)
    # df_each_site.to_csv('avg_month.csv')
    print(all_count)
    third_ = np.percentile(percentile, [75])
    pattern = []
    count_i = 0
    for i in config:
        count_j = 0
        for j in config:
            if i != j:
                if len(all_count[count_i]) >= third_[0] and len(all_count[count_j]) >= third_[0]:
                    df = all_count[count_i].merge(all_count[count_j], on='r_time', how='left')
                    df = df.dropna(axis=0, how='any').reset_index(drop=True)

                    if df.empty:
                        print("is empty")
                    else:
                        df['total'] = np.square(df['count_x'] - df['count_y'])
                        # print(i, j, np.sqrt(df['total'].sum()))
                        pattern.append([i, j, np.sqrt(df['total'].sum())])
                elif len(all_count[count_i]) < third_[0]:
                    print(i)
                    break
            count_j += 1
        count_i += 1
    patternDate = pd.DataFrame(pattern, columns=['place_1', 'place_2', 'total'])
    patternDate.to_csv('pattern_week.csv')
    x.exit()

if __name__ == "__main__":
    main()