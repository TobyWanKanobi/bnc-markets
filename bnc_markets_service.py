import requests, os
from datetime import datetime, date, timedelta
from xml.etree import ElementTree
from xml.etree.ElementTree import ElementTree as ET

namespaces = {'ns0': 'http://s3.amazonaws.com/doc/2006-03-01/'}
filesDownloadUrl = 'https://data.binance.vision/%s'

class BNCMarketsService:

    baseUrl = ''

    def __init__(self, baseUrl):
        self.baseUrl = baseUrl
    
    def downloadFile(self, url, dest):
        
        # Only download if file doesn't exist locally
        if os.path.exists(dest) == False:
            
            print('Downloading: %s' % (url))
            response = requests.get(url)
            
            if response.status_code == 200:
                open(dest, 'wb').write(response.content)
                #print('Checksum For %s = %s' % (file_name, get_checksum(dest, 'sha256')))

    def getDailyTrades(self, symbol, start, end):
        t = start - timedelta(days=1)
        marker = 'data/spot/daily/trades/%s/%s-trades-%s.zip.CHECKSUM' % (symbol, symbol, t.strftime('%Y-%m-%d'))

        print('Request daily trades: %s' % self.baseUrl)
        response = self.__request(self.baseUrl % ('data/spot/daily/trades/%s/&marker=%s' % (symbol, marker)), None)

        xmlDataFiles = ET(ElementTree.fromstring(response.content))
        xmlDataFiles.write("dailytrades.xml")
        fileList = xmlDataFiles.findall('.//ns0:Contents', namespaces)
        
        fl = []
        for file1 in fileList:
            key = file1.find('ns0:Key', namespaces).text
            fileName = key.split("/")[-1]

            if(self.__isfileSkipped(fileName, symbol, start, end)):
                continue

            fl.append(filesDownloadUrl % (key))

        return fl

    # Returns a list of monthly trades
    def getTradeFileList(self, symbol, start, end):

        print('Request monthly trades: %s' % self.baseUrl)
        response = self.__request(self.baseUrl % ('data/spot/monthly/trades/%s/&marker=' % (symbol)), None)

        xmlDataFiles = ET(ElementTree.fromstring(response.content))
        xmlDataFiles.write("monthlytrades.xml")
        fileList = xmlDataFiles.findall('.//ns0:Contents', namespaces)
        
        fl = []
        for file1 in fileList:
            key = file1.find('ns0:Key', namespaces).text
            fileName = key.split("/")[-1]

            if(self.__isfileSkipped(fileName, symbol, start, end)):
                continue

            fl.append(filesDownloadUrl % (key))

        return fl

    def __isfileSkipped(self, fileName, symbol, start, end):

        dateStr = fileName.split('.')[0][len('%s-trades-' % (symbol)):]  # Get date part from filename
        exploded = dateStr.split('-')

        fileDate = None

        if len(exploded) == 2:
            #print('This file probably contains monthly data')
            fileDate  = datetime.strptime(dateStr, '%Y-%m')
        elif len(exploded) == 3:
            #print('This file is probably contains daily data')
            fileDate = datetime.strptime(dateStr, '%Y-%m-%d')

        return False if (fileDate >= start and fileDate  <= end) else True

    def __request(self, url, cb):

        response = requests.get(url)
        print('')
        print('Request: %s' % (url))
        print('Status Code: %s' % (response.status_code))
        print('Content-Type: %s' % (response.headers['content-type']))
        print('')

        if response.status_code == 200:
            return response