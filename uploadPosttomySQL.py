import csv
from dbconnect import connectDB
import time
import datetime
import pytz

config = ["10606", "10607", "10608", "10609", "10610", "10611", "10612", "10701", "10702", "10703", "10704", "10705", "10706"]

PositionforRent = {}
PositionforReturn = {}



def timeSet(temp):
    # 設定時區
    tw = pytz.timezone('Asia/Taipei')
    dt = datetime.datetime.strptime(temp, '%Y/%m/%d %H:%M:%S').replace(tzinfo=tw)
    return time.mktime(dt.timetuple())


def readCsvforRent(x):
    for step in config:
        print(step)
        with open(step + ".csv", newline='') as csvfile:
            global PositionforRent
            PositionforRent = {}
            # 讀取 CSV 檔案內容
            rows = csv.reader(csvfile)
            for row in rows:
                if row[2] == '借車場站':
                    continue

                try:
                    row[1] = row[1].strip()
                    rent_time = timeSet(row[1])
                    # print(rent_time)
                    row[3] = row[3].strip()
                    return_time = timeSet(row[3])
                    # print(return_time)
                    temp = [row[1], rent_time, row[3], return_time, row[4], row[5]]
                    PositionforRent.setdefault(row[2], []).append(tuple(temp))
                except Exception as e:
                    print(e)
        uploadforRent(x, PositionforRent)

def readCsvforReturn(x):
    for step in config:
        print(step)
        with open(step + ".csv", newline='') as csvfile:
            global PositionforReturn
            PositionforReturn = {}
            # 讀取 CSV 檔案內容
            rows = csv.reader(csvfile)
            for row in rows:
                if row[2] == '借車場站':
                    continue

                try:
                    row[1] = row[1].strip()
                    rent_time = timeSet(row[1])
                    # print(rent_time)
                    row[3] = row[3].strip()
                    return_time = timeSet(row[3])
                    # print(return_time)
                    temp = [row[1], rent_time, row[3], return_time, row[2], row[5]]
                    PositionforReturn.setdefault(row[4], []).append(tuple(temp))
                except Exception as e:
                    print(e)
        uploadforReturn(x, PositionforReturn)


def uploadforRent(x, position):

    for key, value in position.items():

        try:
            temp = x.query_data(key)
            snc = temp[0].get('snc')
            snc = snc + "_rent"
            x.create(snc)
        except Exception as e:
            print(e)

        try:
            print(len(tuple(value)))

            if len(tuple(value)) == 1:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-2]

            else:
                temp1 = str(tuple(value))
                temp1 = temp1[1:-1]

            x.insert_data(temp1)
        except Exception as e:
            print(e)

def uploadforReturn(x, position):

    for key, value in position.items():

        try:
            temp = x.query_data(key)
            snc = temp[0].get('snc')
            snc = snc + "_return"
            x.create(snc)
        except Exception as e:
            print(e)

        try:
            print(len(tuple(value)))

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
    # readCsvforRent(x)
    readCsvforReturn(x)
    x.exit()


if __name__ == "__main__":
    main()
