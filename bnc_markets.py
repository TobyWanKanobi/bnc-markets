import io, os, sys, argparse, requests, zipfile
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bnc_markets_service import BNCMarketsService
from utils import get_checksum

# Loading environment variables from .env file
load_dotenv()

parser = argparse.ArgumentParser(description='Personal information')
parser.add_argument('--symbol', dest='symbol', type=str, help='Symbol to dowwnload data for')
parser.add_argument('--start', dest='start', default=datetime.strptime('01-01-2023', '%d-%m-%Y'),type=lambda s: datetime.strptime(s, '%d-%m-%Y'), help='Download trades dating back from')
parser.add_argument('--end', dest='end', default=datetime.strptime('07-04-2023', '%d-%m-%Y'), type=lambda s: datetime.strptime(s, '%d-%m-%Y'), help='Download trades up to')

args = parser.parse_args()

APP_DIRECTORY = os.getcwd()
DOWNLOAD_DIRECTORY = '/'.join((APP_DIRECTORY, os.getenv('DOWNLOAD_DIRECTORY')))
DAILY_DIRECTORY = '/'.join((DOWNLOAD_DIRECTORY, 'daily'))
MONTHLY_DIRECTORY = '/'.join((DOWNLOAD_DIRECTORY, 'monthly'))
PERFORM_CHECKSUM = os.getenv('PERFORM_CHECKSUM')

BNC_MARKETS_SERVICE = BNCMarketsService(os.getenv('BUCKET_STORAGE_URL'))

sys.stdout.write("Try to download marketdata for %s \n" % args.symbol)
print('Downloading %s trades between: startdate=%s enddate=%s' % (args.symbol, args.start, args.end))

def fetchMarketData():
    
    # Request data files for specified symbol
    monthlyTradeFiles = BNC_MARKETS_SERVICE.getTradeFileList(args.symbol, args.start, args.end)
    
    lastEntry = monthlyTradeFiles[-1]
    fileName = lastEntry.split("/")[-1] # Get filename from key
    dateStr = fileName.split('.')[0][len('%s-trades-' % (args.symbol)):]  # Get date part from filename
    
    dailyTradeFiles = BNC_MARKETS_SERVICE.getDailyTrades(args.symbol,  datetime.strptime(dateStr, '%Y-%m') +relativedelta(months=1), args.end)
    return monthlyTradeFiles + dailyTradeFiles

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

for dl in downloadFiles:
    
    file_name = dl.split("/")[-1]
    dateStr = file_name.split('.')[0][len('%s-trades-' % (args.symbol)):]  # Get date part from filename
    frequency = 'daily'

    if len(dateStr) > 7:
        
        # Create symbol folder if it doesn't exist
        if not os.path.exists('/'.join((DAILY_DIRECTORY, args.symbol))):
            os.mkdir('/'.join((DAILY_DIRECTORY, args.symbol)))
    
    else:
        frequency = 'monthly'
        #Create symbol folder if it doesn't exist
        if not os.path.exists('/'.join((MONTHLY_DIRECTORY, args.symbol))):
            os.mkdir('/'.join((MONTHLY_DIRECTORY, args.symbol)))

    # Download data file if it doesnt't exist locally
    fp = '/'.join((DOWNLOAD_DIRECTORY, frequency, args.symbol, file_name))

    BNC_MARKETS_SERVICE.downloadFile(dl, fp)

    if PERFORM_CHECKSUM and fp.split('.')[-1] == 'CHECKSUM':
        checksumFile = open(fp, "r")
        checksum = checksumFile.readline().split(' ')[0]
         
        if get_checksum(fp.replace('.CHECKSUM', ''), 'SHA256') == checksum:
            print('Checksum OK for %s' % fp)
        else:
            print('Checksum incorrect for file %s' % fp)
    
    elif fp.split('.')[-1] == 'zip':
        print('Unzipping %s' % fp)
        with zipfile.ZipFile(fp, 'r') as z:
            z.extractall('/'.join((DOWNLOAD_DIRECTORY, frequency, args.symbol)))