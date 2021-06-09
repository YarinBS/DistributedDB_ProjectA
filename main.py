import pyodbc as db
import datetime
import glob

# X = 5 Category identifier
# Y = 6 Types of items
# Z = 51 -> 51*10 = 510 items i the inventory at the beginning of each day
# => 85 of each item

# Connecting to DB
connection = db.connect('DRIVER={SQL Server};'
                        'SERVER=XXX;'
                        'DATABASE=XXX;'
                        'UID=XXX;'
                        'PWD=XXX')
cursor = connection.cursor()


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


def update_Log(transactionID, record, relation, productID, action):
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


def manage_transactions(T):
    """
    Attempts to perform the order in T seconds, unless aborts
    :param T: amount of time (in seconds) in which the transaction must finish, otherwise it aborts
    :return:
    """
    # TODO: Implement this function
    pass


def main():
    # TODO: Complete main

    # create_tables()
    update_inventory('dsdsdssdsd')


if __name__ == '__main__':
    main()


"""
def delSpesificLine():
    os.chdir("C:\\Users\\Roi\\Desktop\\dimonds\\sequence")
    i = 1
    for file in glob.glob("*.out"):
        with open(file) as f:
            lines = f.readlines()
            f.close()
            del lines[1]
            new_file = open(file + ".ans", "w+")
            for line in lines:
                new_file.write(line)
            new_file.close()
        i = i + 1
"""
