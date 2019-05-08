from lxml import etree, html
import requests, json
import googlemaps
from dbconnect import connectDB


def readurl(x, count):
    headers = {
        'user-agent': '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
    result = requests.get("http://wa.taipei.youbike.com.tw/station/list", headers=headers)
    result.encoding = 'utf8'
    root = etree.fromstring(result.content, etree.HTMLParser())

    for row in root.xpath("//tbody[@id='setarealist']/tr"):
        column = row.xpath("./td/text()")
        column1 = row.xpath("./td/a/text()")
        sarea = column[0]
        sna = column1[0]
        ar = column1[2].strip()
        try:
            geocode_result = gmaps.geocode("YouBike "+column1[0])
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
            snc = "A" + str(count)
            print(snc)
            x.insert_newPost_data(snc, sna, sarea, ar, lat, lng)
            count += 1
        except Exception as e:
            print(e)


def main():
    x = connectDB()
    count = 1
    readurl(x, count)


if __name__ == "__main__":
    main()
