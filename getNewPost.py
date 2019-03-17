from lxml import etree, html
import requests, json
import googlemaps
from dbconnect import connectDB



def readurl(x,count):

    headers = {'user-agent': '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    result = requests.get("http://wa.taipei.youbike.com.tw/station/list",headers = headers)
    result.encoding = 'utf8'
    root = etree.fromstring(result.content, etree.HTMLParser())
    gmaps = googlemaps.Client(key='AIzaSyBccxt1KtwhS1G6xgIBLFZ8VQpFAAtAYns')

    for row in root.xpath("//tbody[@id='setarealist']/tr"):
        column = row.xpath("./td/text()")
        column1 = row.xpath("./td/a/text()")
        sarea = column[0]
        sna = column1[0]
        ar = column1[2].strip()
        try:
            geocode_result = gmaps.geocode(column1[2])
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
            snc = "A" + str(count)
            x.insert_data(snc, sna, sarea, ar, lat, lng)
            count += 1
        except Exception as e:
            print(e)


def main():
    x = connectDB()
    count = 1
    readurl(x,count)

if __name__ == "__main__":
    main()