import csv
from dbconnect import connectDB

config = "youbikeStation.csv"

def main():
  x = connectDB()
  county = ['信義區', '大安區', '南港區', '內湖區', '中正區', '中山區', '文山區', '北投區', '士林區', '大同區', '萬華區', '松山區']
  count = 1
  readCsv(x,county,count)


def readCsv(x,county,count):

  with open(config, newline='') as csvfile:

    # 讀取 CSV 檔案內容
    rows = csv.reader(csvfile)

    # 以迴圈輸出每一列
    for row in rows:
      if row[2] in county:
        snc = "A" + str(count)
        x.insert_data(snc, row[1], row[2], row[3], row[7], row[8])
        count += 1

    x.exit()

if __name__ == "__main__":
    main()


