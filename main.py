import pyodbc as db
import datetime
import glob
import time
import concurrent.futures

# import create_random_orders


# Connecting to DB
connection = db.connect('DRIVER={SQL Server};'
                        'SERVER=XXX;'
                        'DATABASE=XXX;'
                        'UID=XXX;'
                        'PWD=XXX')
cursor = connection.cursor()

connection2 = db.connect('DRIVER={SQL Server};'
                         'SERVER=XXX;'
                         'DATABASE=XXX;'
                         'UID=XXX;'
                         'PWD=XXX')
cursor2 = connection.cursor()


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
    # setting time vaiables
    start = time.perf_counter()
    end = start + 20
    lockFree = True
    for productID in range(1, 7):
        try:
            cursor.execute("insert into ProductsInventory(productID, inventory) values ({}, 85)".format(productID))
            update_Log(transactionID,
                       str("insert into ProductsInventory(productID, inventory) values ({}, 85)".format(productID)),
                       relation="ProductsInventory", productID=productID, action="insert", cursor=cursor,
                       connection=connection)
            connection.commit()
        except Exception as e:
            if check_locks(('XXX', productID)) == 'Unlocked':
                lock('write', 'XXX', productID, transactionID)
            else:
                lockFree = False
                lock_holder = cursor.execute(
                    "select transactionID from Locks where productID = {}".format(
                        productID)).fetchval()[-1]
                if lock_holder == '5':  # Our lock, can be safely removed
                    lockFree = True
                    remove_locks(('yarinbs', productID, 5, "clearmylocks"))
                else:
                    print('Foreign lock on item! Waiting for removal...')
                    while end - time.perf_counter() > 0 or lockFree:
                        print('Time left: ', end='')
                        print(end - time.perf_counter())
                        if check_locks(('XXX', productID)) == 'Unlocked':
                            lockFree = True
                    if not lockFree:
                        print("Lock was not removed, aborting inventory update for this item", productID)
                        continue
            update_Log(transactionID,
                       str("update ProductsInventory set inventory = 85 where productID = {}".format(productID)),
                       relation="ProductsInventory", productID=productID, action="update", cursor=cursor,
                       connection=connection)
            cursor.execute("update ProductsInventory set inventory = 85 where productID = {}".format(productID))
            connection.commit()
            # Remove the writelock
            remove_locks(('XXX', productID, 5, transactionID))  # (serverName, item, amount, fileName)


def update_Log(transactionID, record, relation, productID, action, cursor, connection):
    """
    :param transactionID: a string of 30 characters at most
    :param relation: The relation the SQL statement used
    :param productID:
    :param action: a string from [read, update, delete, insert]
    :param record: SQL statement
    :return:
    """
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


def create_parallel_unit(item, func, time, stage=0):
    """

    :param item:
    :param func:
    :param time:
    :param stage: stage 1: Convert the number in the csv to a serverName => just move to the next Transaction
                  stage 2: Check if locks exists on relevant items => just move to the next Transaction
                  stage 3: Inventory check - check if there's a sufficient amount in the server's inventory => remove applied read locks
                  stage 4: Delete write locks due to insufficient inventory, not time-related error => Cannot be aborted
                  stage 5: Commit => Remove aplied locks and undo if needed
                  stage 6: Delete write locks due and revet on error, not time-related error => Cannot be aborted
    :return:
    """
    dict = {}
    print(time)
    if time <= 0:
        print("negative time")
        return dict

    try:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = executor.map(func, item, timeout=time)
            for server in item:
                dict[server] = next(results)
    except concurrent.futures.TimeoutError as e:
        if stage == 1 or stage == 2:
            print("aborted in stage {} due to timeout".format(stage))
            return
        if stage == 3 or stage == 5:
            print("aborted in stage {} due to timeout".format(stage))
            with concurrent.futures.ProcessPoolExecutor() as executor:
                results = executor.map(remove_locks, item)
            if not dict:
                return
            return dict
    return dict


def Abort(tup):
    """
    Aborts a transaction
    :param tup: (serverName, item, amount, fileName)
    :return: -
    """
    DBcursor, DBconnection = connect(tup[0])

    update_Log(tup[3], str(""""update ProductsInventory set inventory = ? where productID = ?",
                                                                                                 (curr_amount - int(values[2]), int(values[1]))"""),
               'ProductsInventory', tup[1], 'update', DBcursor, DBconnection)

    curr_amount = DBcursor.execute("select inventory from ProductsInventory where productID = ?",
                                   (tup[1])).fetchval()

    DBcursor.execute("update ProductsInventory set inventory = ? where productID = ?",
                     (curr_amount + int(tup[2]), tup[1]))
    DBconnection.commit()

    return 'Aborted successfully'


def check_locks(tup):
    """
    Given a tuple (serverName, item), checks whether the item is locked on the serverName's server
    (e.g. has locks in the Locks table)
    :param tup: (serverName, item)
    :return: 'Locked' if has locks, 'Unlocked' otherwise
    """
    DBcursor, DBconnection = connect(tup[0])

    num_of_read_locks = DBcursor.execute(
        "select count(transactionID) from Locks where productID = {} and lockType = 'read'".format(tup[1])).fetchval()

    num_of_write_locks = DBcursor.execute(
        "select count(transactionID) from Locks where productID = {} and lockType = 'write'".format(tup[1])).fetchval()

    if num_of_write_locks:
        return 'Writelocked'
    elif num_of_read_locks > 1:
        return 'MultiReadlocked'
    elif num_of_read_locks > 0:
        return 'SingleReadlocked'
    else:
        return 'Unlocked'


def lock(lockType, serverName, item, fileName):
    """
    Applies lockType lock on item on serverName's server
    :param lockType: String, either 'read' or 'write'
    :param serverName: server name to connect to
    :param item: item to lock
    :param fileName: transactionID for the Locks table
    :return: -
    """
    # Connect to server
    # DBcursor, DBconnection = connect(serverName)
    cursor.execute("insert into Locks(transactionID, productID, lockType) values (?,?,?)",
                   (fileName, item, lockType))
    connection.commit()
    # Update log
    update_Log(fileName,
               str("insert into Locks(transactionID, productID, lockType) values (?,?,?)"),
               relation="Locks", productID=item, action="insert", cursor=cursor,
               connection=connection)

    print(lockType + ' lock applied on ' + serverName + '\'s item ' + str(item))


def remove_suffix(text, suffix="\n"):
    if text.endswith(suffix):
        text = text[:-1]
    return text


def remove_prefix(text, prefix="orders\\"):
    return text[text.startswith(prefix) and len(prefix):][:-4]


def check_inventory(tup):
    """
    Given a tuple (serverName, item, amount, fileName), checks whether the amount of item 'item' in serverName's inventory
    is sufficient for the transaction. Also locks the item with a readlock.
    :param tup: (serverName, item, amount, fileName)
    :return:
    """
    DBcursor, DBconnection = connect(tup[0])
    # Lock the item for inspection
    try:
        DBcursor.execute(
            "insert into Locks(transactionID, productID, lockType) values (?,?,?)", (tup[3], tup[1], 'read'))
        DBconnection.commit()
        update_Log(tup[3], str("insert into Locks(transactionID, productID, lockType) values (?,?,?)"),
                   relation='Locks', productID=tup[1], action='insert', cursor=DBcursor, connection=DBconnection)
    except Exception as e:
        print("Error: Primary Key Violation")
        return 'Invalid'

    # Inspect amount
    try:
        amount_in_inventory = DBcursor.execute(
            "select inventory from ProductsInventory where productID = {}".format(tup[1])).fetchval()
        update_Log(tup[3], str("select inventory from ProductsInventory where productID = {}"),
                   relation='ProductsInventory', productID=tup[1], action='read',
                   cursor=DBcursor, connection=DBconnection)
    except Exception as e:
        return 'Invalid'

    if amount_in_inventory < int(tup[2]):
        return 'Invalid'
    else:
        return 'Valid'


def Commit(tup):
    """
    Upgrades readlocks to writelocks and updates the inventory
    :param tup: (serverName, item, amount, fileName)
    :return:
    """
    if check_locks(tup) != 'SingleReadlocked':
        remove_locks(tup)
        return 'Aborted'

    DBcursor, DBconnection = connect(tup[0])

    # Upgrade our readlock to writelock
    DBcursor.execute("update Locks set lockType = 'write' where productID = ? and transactionID = ?",
                     (tup[1], tup[3]))
    DBconnection.commit()

    # Update inventory
    try:
        update_Log(tup[3], str(""""update ProductsInventory set inventory = ? where productID = ?",
                                                                                             (curr_amount - int(values[2]), int(values[1]))"""),
                   'ProductsInventory', tup[1], 'update', DBcursor, DBconnection)

        curr_amount = DBcursor.execute("select inventory from ProductsInventory where productID = ?",
                                       (tup[1])).fetchval()

        DBcursor.execute("update ProductsInventory set inventory = ? where productID = ?",
                         (curr_amount - int(tup[2]), tup[1]))
        DBconnection.commit()
    except Exception as e:
        remove_locks(tup)
        return 'Aborted'

    # Remove writelock
    remove_locks(tup)

    return "committed successfully"


def remove_locks(tup):
    """
    Given a tuple (serverName, item, amount, fileName), removes the lock tuple from the Locks table
    :param tup: (serverName, item, amount, fileName)
    :return: -
    """
    # Connect to server
    DBcursor, DBconnection = connect(tup[0])

    try:

        if tup[3] == 'clearmylocks':
            # Removes the tuple from the table
            DBcursor.execute("delete from Locks where productID = ?", (tup[1]))
            DBconnection.commit()
        else:
            # Update log
            update_Log(tup[3], str("delete from Locks where transactionID = ? and productID = ?"), relation='Locks',
                       productID=tup[1], action='delete', cursor=DBcursor, connection=DBconnection)
            # Removes the tuple from the table
            DBcursor.execute("delete from Locks where transactionID = ? and productID = ?", (tup[3], tup[1]))
            DBconnection.commit()
    except Exception as e:
        return
    return


def manage_transactions(T):
    success = []
    failed = []
    for file in sorted(glob.glob("orders/*_5.csv")):
        failed.append(remove_prefix(file))
        start = time.perf_counter()
        end = start + T
        print(start)
        data_triplets = []
        with open(file) as f:
            for line in f.readlines():
                values = line.split(',')
                values.append(remove_prefix(file))
                if values[0] == 'categoryID':
                    continue
                else:
                    data_triplets.append(values)
        server_nums = [item[0] for item in data_triplets]
        dict_of_servers = create_parallel_unit(server_nums, find_DB_name, time=end - time.perf_counter(), stage=1)
        if dict_of_servers is None:
            continue
        for i in range(len(data_triplets)):
            data_triplets[i][0] = dict_of_servers[data_triplets[i][0]]
            data_triplets[i][2] = remove_suffix(data_triplets[i][2], "\n")
            data_triplets[i] = tuple(data_triplets[i])
        flag = 1
        while flag == 1:
            dict_of_stats = create_parallel_unit(data_triplets, check_locks, time=end - time.perf_counter(), stage=2)
            if dict_of_stats is None:
                continue
            print(dict_of_stats)
            if not all(value == "Unlocked" or value == "MultiReadlocked" or value == "SingleReadlocked" for value in
                       dict_of_stats.values()):
                print(remove_prefix(file),
                      " Transaction can't be made as an Atomic unit due to a Write lock on one of the products")
                continue
            flag = 0
        dict_of_stats = create_parallel_unit(data_triplets, check_inventory, time=end - time.perf_counter(), stage=3)
        if dict_of_stats is None:
            continue
        print(dict_of_stats)

        if not all(value == "Valid" for value in dict_of_stats.values()):
            dict_of_stats = create_parallel_unit(data_triplets, remove_locks, time=9999, stage=4)
            # Abort transaction
            print(remove_prefix(file), 'Aborted due insufficient inventory or readLock issues')
            continue
        dict_of_stats = create_parallel_unit(data_triplets, check_locks, time=end - time.perf_counter(), stage=2)
        if dict_of_stats is None:
            continue
        if not all(value == "SingleReadlocked" for value in
                   dict_of_stats.values()):
            print("Transaction can't be made as an Atomic unit due to a foreign lock")
            continue
        dict_of_stats = create_parallel_unit(data_triplets, Commit, time=end - time.perf_counter(), stage=5)
        if dict_of_stats is None:
            continue
        print(dict_of_stats)
        if not all(value == "committed successfully" for value in dict_of_stats.values()):
            products_to_revet = {k: v for k, v in dict_of_stats.items() if v == "committed successfully"}
            dict_of_stats = create_parallel_unit(products_to_revet, Abort, time=9999, stage=6)
            continue
        print(remove_prefix(file), " WHOLE transaction was Atomic Committed")
        success.append(failed.pop(-1))

    # Print successful/failed transactions to console
    print("Successful transaction: ", end='')
    for transaction in success:
        print(transaction, end=' ')
    print()
    print("Failed transaction: ", end='')
    for transaction in failed:
        print(transaction, end=' ')


def main():
    # create_random_orders

    # create_tables()
    # update_inventory('updatasdfingntory')
    manage_transactions(30)

    # cursor.execute("insert into Locks(transactionID, productID, lockType) values ('apjh54645a54486sdfgb164', 5, 'write')")
    # cursor.commit()

    # DBcursor, DBconnection = connect('XXX')
    # DBcursor.execute("insert into Locks(transactionID, productID, lockType) values ('<3', 3, 'write')")
    # DBcursor.execute("insert into Locks(transactionID, productID, lockType) values ('<3', 2, 'read')")

    # DBcursor.execute("delete from Locks where transactionID = 'abcd'")

    # DBconnection.commit()

    #
    # print(check_locks(('yarinbs', 5)))
    # print(check_locks(('yarinbs', 6)))
    # print(check_locks(('yarinbs', 1)))
    #
    # parallel_manage_transactions(30)

    # remove_locks(('yarinbs', 6, 8, 'XYZ_5'))  # (serverName, item, amount, fileName)
    pass


if __name__ == '__main__':
    main()
