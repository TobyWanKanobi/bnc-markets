import os, csv
from  bnc_market_repo import MarketRepository

MAX_BULK_INSERT_AMOUNT = 1000

class TradeSeeder:

    __MARKET_REPOSITORY = None

    def __init__(self, options):

        print('New TradeSeeder instance')
        self.__MARKET_REPOSITORY = MarketRepository(options)

    def seed(self, fp):

        print('Seeding file: %s' % fp)

        if not os.path.exists(fp):
            print('Error csv not exist')
        
        tname = fp.split('/')[-1].split('-')[0].lower()
        self.__MARKET_REPOSITORY.createTable(tname)
        
        # Open CSV file
        with open(fp) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0

            batch =[]

            '''
                row structure: (
                    trade ID,
                    price,
                    quantity,
                    quote quantity,
                    time in unix time format,
                    was the buyer the maker,
                    was the trade the best price match)"""
            '''
            for row in csv_reader:
                batch.append(row)

                line_count += 1

                # Insert batch when MAX_BULK_INSERT_AMOUNT is reached
                if len(batch) == MAX_BULK_INSERT_AMOUNT:
                    self.__MARKET_REPOSITORY.bulkInsert(tname, batch)
                    batch.clear()

            # Insert any leftover in the batch
            if(batch):
                self.__MARKET_REPOSITORY.bulkInsert(tname, batch)

            return line_count