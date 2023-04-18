import mysql.connector

class MarketRepository:

    __cnx = None
    __options = None

    def __init__(self, options):

        print('New MarketRepository instance')

        self.__options = options

        self.__cnx = mysql.connector.connect(
            user=options['DB_USER'],
            password=options['DB_PASSWORD'],
            host=options['DB_HOST'],
            port=options['DB_PORT'],
            database=options['DB_NAME'])

    def getAllTables(self):
        
        query = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='%s'") % (self.__options['DB_NAME'])

        self.execute(query)

        cursor = self.__cnx.cursor()
        for r in cursor:
            print(r)

    def execute(self, query):
        cursor = self.__cnx.cursor()
        cursor.execute(query)
    
    def createTable(self, symbol):

        query1 = """
        CREATE TABLE IF NOT EXISTS `%s`.`binance_trades_%s` (
            `id` bigint(20) NOT NULL,
            `price` varchar(30) NOT NULL,
            `qty` varchar(30) NOT NULL,
            `quoteQty` varchar(30) NOT NULL,
            `time` bigint(20) NOT NULL,
            `isBuyerMaker` BOOLEAN NOT NULL,
            `isBestMatch` BOOLEAN NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;""" % (self.__options['DB_NAME'], symbol)

        self.execute(query1)

    def bulkInsert(self, symbol, list):

        values = []
        for r in list:
            values.append('(%s, %s, %s, %s, %s, %s, %s)' % (r[0], r[1], r[2], r[3], r[4], r[5], r[6]))

        query =  """INSERT INTO `%s`.`binance_trades_%s` (id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch) VALUES %s;""" % (self.__options['DB_NAME'], symbol, ','.join(values))
        self.execute(query)

    def destroy(self):
        self.__cnx.cursor.close()
        self.__cnx.close() 

    

