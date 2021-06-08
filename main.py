import pyodbc as db


def create_tables():
    # Connecting to DB
    connection = db.connect('DRIVER={SQL Server};'
                      'SERVER=technionddscourse.database.windows.net;'
                      'DATABASE=yarinbs;'
                      'UID=yarinbs;'
                      'PWD=Qwerty12!')
    cursor = connection.cursor()

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
            rowID int PRIMARY KEY,
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
    #TODO: Implement this function
    pass


def manage_transactions(T):
    # TODO: Implement this function
    pass


def main():
    create_tables()
    # TODO: Complete main


if __name__ == '__main__':
    main()