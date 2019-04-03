import csv
from dbconnect import connectDB
import time
import datetime
import pytz

config = ["10606", "10607", "10608", "10609", "10610", "10611", "10612", "10701", "10702", "10703", "10704", "10705", "10706"]
# config = ["test", "test1"]


PositionForRent = {}
PositionForReturn = {}

# 創過的資料表
county_site = []

# 站點名稱改變
different = {"萬華車站": "萬大興寧街口",
             "捷運麟光站2號出口": "捷運麟光站",
             "饒河夜市": "饒河夜市(八德路側)",
             "萬大興寧街口": "萬華車站",
             "捷運麟光站": "捷運麟光站2號出口",
             "饒河夜市(八德路側)": "饒河夜市"}

# 沒有出現的地點
del_for_never = []


def TimeSet(temp):
    # 設定時區
    tw = pytz.timezone('Asia/Taipei')
    dt = datetime.datetime.strptime(temp, '%Y/%m/%d %H:%M:%S').replace(tzinfo=tw)
    return time.mktime(dt.timetuple())


def ReadCsvForRent(x):
    for step in config:
        print(step)

        global PositionForRent
        PositionForRent = {}

        with open("./csv/" + step + ".csv", newline='') as csvfile:

            # 讀取 CSV 檔案內容
            rows = csv.reader(csvfile)
            for row in rows:

                # 跳過第一行
                if row[2] == '借車場站':
                    continue
                try:
                    row[1] = row[1].strip()
                    rent_time = TimeSet(row[1])
                    # print(rent_time)
                    row[3] = row[3].strip()
                    return_time = TimeSet(row[3])
                    # print(return_time)
                    temp = [row[1], rent_time, row[3], return_time, row[4], row[5]]
                    # 創建list存放每一個站點各自的資料
                    PositionForRent.setdefault(row[2], []).append(tuple(temp))
                except Exception as e:
                    print(e)
        UploadForRent(x, PositionForRent)


def ReadCsvForReturn(x):
    for step in config:
        print(step)

        global PositionForReturn
        PositionForReturn = {}

        with open(step + ".csv", newline='') as csvfile:

            # 讀取 CSV 檔案內容
            rows = csv.reader(csvfile)
            for row in rows:
                if row[2] == '借車場站':
                    continue
                try:
                    row[1] = row[1].strip()
                    rent_time = TimeSet(row[1])
                    # print(rent_time)
                    row[3] = row[3].strip()
                    return_time = TimeSet(row[3])
                    # print(return_time)
                    temp = [row[1], rent_time, row[3], return_time, row[2], row[5]]
                    PositionForReturn.setdefault(row[4], []).append(tuple(temp))
                except Exception as e:
                    print(e)
        UploadForReturn(x, PositionForReturn)


def UploadForRent(x, position):
    global county_site, different, del_for_never

    for key, value in position.items():

        # 去比對每一個是否是改名站點
        if key in different:
            temp = x.query_data(key)
            if not temp:
                temp = x.query_data(different[key])
        else:
            temp = x.query_data(key)

        # 若info找不到就會去create新的資料表 ＆ create info裡面的資料
        if not temp:
            # snc = x.query_data_count()
            # snc = snc[0].get('COUNT(*)')
            # snc += 1
            # county_site.append(snc)
            # x.insert_newPost_data(snc, key, "", "", 0, 0)
            # snc = "A" + str(snc) + "_rent"
            # x.create(snc)
            del_for_never.append(key)

        # 若有在info搜尋到 就會create資料表
        elif temp:
            snc = temp[0].get('snc')
            if snc not in county_site:
                county_site.append(snc)
                snc = snc + "_rent"
                x.create(snc)
            else:
                # 指向資料表
                snc = snc + "_rent"
                x.getcode(snc)

        # 上傳資料
        try:
            print(len(tuple(value)))
            # 若只有一筆
            if len(tuple(value)) == 1:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-2]
            # 一筆以上
            else:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-1]

            x.insert_data(temp1)
        except Exception as e:
            print(e)

        # print(county_site)


def UploadForReturn(x, position):
    global county_site
    for key, value in position.items():

        if key in different:
            temp = x.query_data(key)
            if not temp:
                temp = x.query_data(different[key])
        else:
            temp = x.query_data(key)

        if not temp:
            # snc = x.query_data_count()
            # snc = snc[0].get('COUNT(*)')
            # snc += 1
            # county_site.append(snc)
            # x.insert_newPost_data(snc, key, "", "", 0, 0)
            # snc = "A" + str(snc) + "_rent"
            # x.create(snc)
            del_for_never.append(key)
        elif temp:
            snc = temp[0].get('snc')
            if snc not in county_site:
                county_site.append(snc)
                snc = snc + "_return"
                x.create(snc)
            else:
                # 指向資料表
                snc = snc + "_return"
                x.getcode(snc)

        try:
            if len(tuple(value)) == 1:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-2]
            else:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-1]

            x.insert_data(temp1)
        except Exception as e:
            print(e)


def main():
    x = connectDB()
    # ReadCsvForRent(x)
    ReadCsvForReturn(x)
    x.exit()
    print(del_for_never)


if __name__ == "__main__":
    main()
