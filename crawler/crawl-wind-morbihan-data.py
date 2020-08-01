# Semaphore d'Etel - https://www.windmorbihan.com/node/73091277/historique/
# https://www.windmorbihan.com/node/73091277/historique/1595966400
from bs4 import BeautifulSoup
from urllib import request
from datetime import datetime
import time
import json
import sqlite3

DAY_IN_SECONDS = 24 * 60 * 60
PLACES = {
  "etel": 73091277
}

def crawlLoop(dbCursor, place, placeCode, epochDateStart, epochDateEnd):
  for epochDate in range(epochDateStart, epochDateEnd, DAY_IN_SECONDS):
    for dataEntry in parseResponse(epochDate, crawlOnePage(placeCode, epochDate)):
      print(dataEntry)
      insertData(dbCursor, place, *dataEntry)

def crawlOnePage(placeCode, epochDate):
  print(time.strftime('%Y-%m-%d', time.localtime(epochDate)))
  response = request.urlopen("https://www.windmorbihan.com/node/%d/historique/%d" % (placeCode, epochDate))
  return response.read()

def parseResponse(epochDate, content):
  soup = BeautifulSoup(content, 'html.parser')
  # Speed
  data = soup.find(id="highchart-render")
  if not data:
    return []
  parsed = json.loads(data.attrs['data-chart'])
  seriesData1 = parsed['series'][0]['data']
  # return seriesData
  #print(json.dumps(parsed, indent=2, sort_keys=True))
  # Direction
  data = soup.find(id="highchart-render--2")
  parsed = json.loads(data.attrs['data-chart'])
  seriesData2 = parsed['series'][0]['data']
  seriesData = []
  date1 = datetime.fromtimestamp(epochDate)
  for i in range(len(seriesData1)):
    day = int(seriesData1[i][0].split(" ")[0])
    time = seriesData1[i][0].split(" ")[2]
    date2 = datetime.strptime(time, "%H:%M:%S")
    print(seriesData1[i][0])
    date = date2.replace(year=date1.year, month=date1.month)
    seriesData.append([int(date.timestamp()), seriesData1[i][1], seriesData2[i][1]])
  return seriesData

def insertData(dbCursor, place, date, speed, direction):
  dbCursor.execute("INSERT INTO wind (place, date, speed, direction) VALUES (?, ?, ?, ?)", (place, date, speed, direction))

if __name__ == "__main__":
  # 2019-07-29 -> 2020-07-29
  # crawlLoop(1564426000, 1595966400)
  db = sqlite3.connect('wind-morbihan-data.db')
  dbCursor = db.cursor()
  dbCursor.execute('CREATE TABLE IF NOT EXISTS wind(id INTEGER PRIMARY KEY AUTOINCREMENT, place TEXT NOT NULL, date INTEGER NOT NULL, speed REAL NOT NULL, direction REAL NOT NULL)')
  for place in PLACES:
    crawlLoop(dbCursor, place, PLACES[place], 1564437600, 1595966400) #1564512400
  db.commit()
  dbCursor.close()
