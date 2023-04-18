import os, sys, argparse, zipfile, time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bnc_markets_service import BNCMarketsService
from dotenv import dotenv_values
from utils import get_checksum
from trade_seeder import TradeSeeder

config = dotenv_values()

APP_DIRECTORY = os.getcwd()
DOWNLOAD_DIRECTORY = '/'.join((APP_DIRECTORY, config['DOWNLOAD_DIRECTORY']))
DAILY_DIRECTORY = '/'.join((DOWNLOAD_DIRECTORY, 'daily'))
MONTHLY_DIRECTORY = '/'.join((DOWNLOAD_DIRECTORY, 'monthly'))
PERFORM_CHECKSUM = config['PERFORM_CHECKSUM']
BNC_MARKETS_SERVICE = BNCMarketsService(config['BUCKET_STORAGE_URL'])

TRADE_SEEDER = TradeSeeder({
            'DB_USER': config['DB_USER'], 
            'DB_PASSWORD': config['DB_PASSWORD'],
            'DB_HOST': config['DB_HOST'],
            'DB_PORT': config['DB_PORT'], 
            'DB_NAME':config['DB_NAME']
})

# Loading environment variables from .env file


parser = argparse.ArgumentParser(description='Personal information')
parser.add_argument('--symbol', dest='symbol', type=str, help='Symbol to dowwnload data for')
parser.add_argument('--start', dest='start', default=datetime.strptime('01-01-2023', '%d-%m-%Y'),type=lambda s: datetime.strptime(s, '%d-%m-%Y'), help='Download trades dating back from')
parser.add_argument('--end', dest='end', default=datetime.strptime('07-04-2023', '%d-%m-%Y'), type=lambda s: datetime.strptime(s, '%d-%m-%Y'), help='Download trades up to')

args = parser.parse_args()


sys.stdout.write("Try to download marketdata for %s \n" % args.symbol)
print('Downloading %s trades between: startdate=%s enddate=%s' % (args.symbol, args.start, args.end))

def fetchMarketData():
    
    # Request data files for specified symbol
    monthlyTradeFiles = BNC_MARKETS_SERVICE.getMonthlyTrades(args.symbol, args.start, args.end)
    
    lastEntry = monthlyTradeFiles[-1]
    fileName = lastEntry.split("/")[-1] # Get filename from key
    dateStr = fileName.split('.')[0][len('%s-trades-' % (args.symbol)):]  # Get date part from filename
    
    dailyTradeFiles = BNC_MARKETS_SERVICE.getDailyTrades(args.symbol,  datetime.strptime(dateStr, '%Y-%m') +relativedelta(months=1), args.end)
    return monthlyTradeFiles + dailyTradeFiles

def performChecksum(file):
    
    checksumFile = open(file.replace('.zip', '.zip.CHECKSUM'), "r")
    checksum = checksumFile.readline().split(' ')[0]
    
    if get_checksum(file, 'SHA256') == checksum:
        print('Checksum OK for %s' % file)
    else:
        print('Checksum incorrect for file %s' % file)


def createAppFolders():

    # Create data storage folder if it doesnt't exist
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.mkdir(DOWNLOAD_DIRECTORY)

    # Create symbol folder if it doesn't exist
    if not os.path.exists(MONTHLY_DIRECTORY):
        os.mkdir(MONTHLY_DIRECTORY)

    # Create symbol folder if it doesn't exist
    if not os.path.exists(DAILY_DIRECTORY):
        os.mkdir(DAILY_DIRECTORY)

"""
    BinanceMarketData
"""

createAppFolders()
downloadFiles = fetchMarketData()
downloadResult = []

'''
    Download files
'''
for fileSrc in downloadFiles:
    
    file_name = fileSrc.split("/")[-1]
    file_extension = file_name.split('.')[-1]
    dateStr = file_name.split('.')[0][len('%s-trades-' % (args.symbol)):]  # Get date part from filename
    frequency = 'daily'

    if len(dateStr) > 7:
        
        # Create symbol folder if it doesn't exist
        if not os.path.exists('/'.join((DAILY_DIRECTORY, args.symbol))):
            os.mkdir('/'.join((DAILY_DIRECTORY, args.symbol)))
    
    else:
        frequency = 'monthly' 
        # Create symbol folder if it doesn't exist
        if not os.path.exists('/'.join((MONTHLY_DIRECTORY, args.symbol))):
            os.mkdir('/'.join((MONTHLY_DIRECTORY, args.symbol)))

    # Construct filepath
    fileDst = '/'.join((DOWNLOAD_DIRECTORY, frequency, args.symbol, file_name))

    BNC_MARKETS_SERVICE.downloadFile(fileSrc, fileDst)

    if(file_extension == 'zip'):
        downloadResult.append(fileDst)

'''
    Check file integrity of downloaded files
'''
for file in downloadResult:
    performChecksum(file)

'''
    Unzip downloaded zipfiles
'''
for file  in downloadResult:

    extension = file.split('.')[-1]

    if(extension == 'zip' and not os.path.exists(file.replace('zip', 'csv'))):     
        with zipfile.ZipFile(file, 'r') as z:
            print('Unzipping %s' % file)
            z.extractall('/'.join((DOWNLOAD_DIRECTORY, frequency, args.symbol)))

'''
    Insert downloaded csv file in database
'''
for file in downloadResult:
    
    start = time.time()
    recordsInserted = TRADE_SEEDER.seed(file.replace('zip', 'csv'))
    end = time.time()

    print('Records inserted: %s in time:  %s' % (recordsInserted, end-start))