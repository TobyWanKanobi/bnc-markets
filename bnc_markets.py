import io, os, sys, argparse, calendar, requests, zipfile, hashlib
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bnc_markets_service import BNCMarketsService

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

BNC_MARKETS_SERVICE = BNCMarketsService(os.getenv('BUCKET_STORAGE_URL'))

sys.stdout.write("Try to download marketdata for %s \n" % args.symbol)
print('Downloading %s trades between: startdate=%s enddate=%s' % (args.symbol, args.start, args.end))

def get_checksum(filename, hash_function):
    """Generate checksum for file baed on hash function (MD5 or SHA256).
 
    Args:
        filename (str): Path to file that will have the checksum generated.
        hash_function (str):  Hash function name - supports MD5 or SHA256
 
    Returns:
        str`: Checksum based on Hash function of choice.
 
    Raises:
        Exception: Invalid hash function is entered.
 
    """
    hash_function = hash_function.lower()
 
    with open(filename, "rb") as f:
        bytes = f.read()  # read file as bytes
        if hash_function == "md5":
            readable_hash = hashlib.md5(bytes).hexdigest()
        elif hash_function == "sha256":
            readable_hash = hashlib.sha256(bytes).hexdigest()
        else:
            Raise("{} is an invalid hash function. Please Enter MD5 or SHA256")
 
    return readable_hash

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

    if len(dateStr) > 5:
        
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
        
    #z = zipfile.ZipFile(io.BytesIO(downloadzip.content))
    #z.extractall(symbol_data_folder)"""