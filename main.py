import pyodbc as db
import datetime
import glob

"""
TODO:
1. implement locking procedure - add and remove
2. Consider T seconds of transaction
3. iterate twice over the file:
    deadline
    first iteration:
    for each line, if no failures occurres: lock() the product, update_log() and add to locked_sites_by_us
    else, abort() all that was locked 
    second_iteration:
    for each line: update log, update tables and unlock()
4. check time validity at each step
"""

# Connecting to DB
connection = db.connect('DRIVER={SQL Server};'
                        'SERVER=XXX;'
                        'DATABASE=XXX;'
                        'UID=XXX;'
                        'PWD=XXX')
cursor = connection.cursor()


def connect(siteName):
    name = siteName
    connection = db.connect('DRIVER={SQL Server};'
                            'SERVER=XXX;'
                            'DATABASE=' + name + ';'
                                                 'UID=' + name + ';'
                                                                 'PWD=XXX')
    cursor = connection.cursor()

    return cursor, connection


def create_tables():
    """
    Creates the required tables in the DB
    :return: -
    """

    # Creating Tables in the DB
    cursor.execute("""
            
            create table ProductsInventory
            (
            productID int PRIMARY KEY,
            inventory int
                CHECK(inventory > -1)
            )
            
            """
                   )

    cursor.execute("""
    
            create table ProductsOrdered
            (
            transactionID VARCHAR(30),
            productID int,
            amount int
                CHECK(amount > 0),
            PRIMARY KEY(transactionID, productID),
            FOREIGN KEY(productID) REFERENCES ProductsInventory(productID)
            )
            
            """
                   )

    cursor.execute("""
            
            create table Log(
            rowID int IDENTITY(1, 1) PRIMARY KEY,
            timestamp datetime,
            relation VARCHAR(30)
                CHECK(relation = 'ProductsInventory' or relation = 'ProductsOrdered' or relation = 'Locks'),
            transactionID VARCHAR(30),
            productID int,
            action VARCHAR(15)
                CHECK(action = 'read' or action = 'update' or action = 'delete' or action = 'insert'),
            record VARCHAR(2500),
            FOREIGN KEY(productID) REFERENCES ProductsInventory(productID)
            )

            
            """
                   )

    cursor.execute("""
    
            create table Locks(
            transactionID VARCHAR(30),
            productID int,
            lockType VARCHAR(6)
                CHECK(lockType = 'read' or lockType = 'write'),
            PRIMARY KEY(transactionID, productID, lockType),
            FOREIGN KEY(productID) REFERENCES ProductsInventory(productID)
            )
    
    """
                   )

    connection.commit()


def update_inventory(transactionID):
    """
    Updates the inventory
    :param transactionID: a string of 30 characters at most
    :return:
    """
    for productID in range(1, 7):
        try:
            cursor.execute("insert into ProductsInventory(productID, inventory) values ({}, 85)".format(productID))
            update_Log(transactionID,
                       str("insert into ProductsInventory(productID, inventory) values ({}, 85)".format(productID)),
                       relation="ProductsInventory", productID=productID, action="insert")
        except Exception as e:
            cursor.execute("update ProductsInventory set inventory = 85 where productID = {}".format(productID))
            update_Log(transactionID,
                       str("update ProductsInventory set inventory = 85 where productID = {}".format(productID)),
                       relation="ProductsInventory", productID=productID, action="update")

    connection.commit()


def update_Log(transactionID, record, relation, productID, action, cursor, connection):
    """
    :param transactionID: a string of 30 characters at most
    :param relation: The relation the SQL statement used
    :param productID:
    :param action: a string from [read, update, delete, insert]
    :param record: SQL statement
    :return:
    """
    f = '%Y-%m-%d %H:%M:%S'
    ts = datetime.datetime.now()

    cursor.execute(
        "insert into Log(timestamp, relation, transactionID, productID, action, record) values (?,?,?,?,?,?)",
        (ts, relation, transactionID, productID, action, record))
    connection.commit()


def find_DB_name(num):
    # Connect to main server
    connection = db.connect('DRIVER={SQL Server};'
                            'SERVER=XXX;'
                            'DATABASE=XXX;'
                            'UID=XXX;'
                            'PWD=XXX')
    cursor = connection.cursor()

    # Get site name
    siteName = cursor.execute("select siteName from CategoriesToSites where categoryID = {}".format(num)).fetchval()

    return siteName


def valid(curr_amount, order_amount):
    if curr_amount < order_amount:
        return True


def abort(sites_with_our_lock):
    for siteName in sites_with_our_lock:
        DBcursor, DBconnection = connect(siteName)
        # REMOVE LOCKS

    pass


def commit(sites_with_our_lock, values, file):
    for site in sites_with_our_lock:
        DBcursor, DBconnection = connect(site)

        update_Log(file, str(""""update ProductsInventory set inventory = ? where productID = ?",
                                                                                     (curr_amount - int(values[2]), int(values[1]))"""),
                   'ProductsInventory', values[1], 'update', DBcursor, DBconnection)

        curr_amount = DBcursor.execute("select inventory from ProductsInventory where productID = ?",
                                       (int(values[1]))).fetchval()

        DBcursor.execute("update ProductsInventory set inventory = ? where productID = ?",
                         (curr_amount - int(values[2]), int(values[1])))
        DBconnection.commit()


def manage_transactions(T):
    """
    Attempts to perform the order in T seconds, unless aborts
    :param T: amount of time (in seconds) in which the transaction must finish, otherwise it aborts
    :return:
    """
    deadline = T + datetime.datetime.now()
    # TODO: Implement this function
    for file in glob.glob("orders/*_5.csv"):
        with open(file) as f:
            sites_with_our_lock = []
            for line in f.readlines():
                values = line.split(',')
                if values[0] == 'categoryID':
                    continue
                else:
                    # Connect to DB
                    siteName = find_DB_name(values[0])
                    print('Hijacking from ', end='')
                    print(siteName + '...')
                    DBcursor, DBconnection = connect(siteName)

                    # Check ProductInventory table
                    curr_amount = DBcursor.execute("select inventory from ProductsInventory where productID = ?",
                                                   (int(values[1]))).fetchval()
                    print("{}'s inventory has {} items of productID {}".format(siteName, curr_amount, values[1]))
                    if not valid(curr_amount, int(values[2])):
                        abort()

                    else:
                        if siteName not in sites_with_our_lock:
                            #TODO: APPLY LOCK ON SITE
                            sites_with_our_lock.append(siteName)
                        DBcursor.execute("update ProductsInventory set inventory = ? where productID = ?",
                                         (curr_amount - int(values[2]), int(values[1])))
                        DBconnection.commit()

                        update_Log(file, str(""""update ProductsInventory set inventory = ? where productID = ?",
                                                                     (curr_amount - int(values[2]), int(values[1]))"""),
                                   'ProductsInventory', values[1], 'update', DBcursor, DBconnection)
                    print("{} items have been hijacked".format(values[2][:-1]))
                    print()


def main():
    # TODO: Complete main

    # create_tables()
    # update_inventory('dsdsdssdsd')
    manage_transactions(1000)


if __name__ == '__main__':
    print('hi')
    main()
