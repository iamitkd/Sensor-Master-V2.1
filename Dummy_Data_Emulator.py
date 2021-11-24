from threading import Thread
import threading
import asyncore
import socket
import struct
import time
import struct
import copy
import json
import os.path
import mysql.connector
from mysql.connector import connect
import os,binascii
import codecs
rnge1 = 1
rnge2 = 226
ID = 112

WAIT_TIME = 0.02
T_lock = threading.RLock()

# Sensor states
NORMAL = 1
VIBRATION_ALARM = 3
TEMPERATURE_ALARM = 4
NO_RESPONSE = 0

# Alarm countdown interval
ALARM_CNT = 0

# Sensor Channel dictionaries
channl = {}
channl2 ={}
channl3 ={}

sensor_map = {}
master_id = -1

# looping threads for polling sensors on individual sensor channels
def poll_sensors(a):
    # Global Variables
    global sensor_map

    # Attempt database connection
    connection1 = connect(host=a,database=database_name, user=db_userid, password=db_password,autocommit=True)
    # connection2 = connect(host="192.168.5.126", database=database_name, user=db_userid, password=db_password)

    # Create cursor object
    cursor1 = connection1.cursor(prepared=True)

    i = 0
    data  = ''
    for i in range(256):
        #n = (i.to_bytes(1, 'big') * 5)[2:-1]
        if i == 39 or i == 92:
            n = ('\\'+chr(i))*5
        else:
            n = chr(i) * 5
        #print((n.encode()))
        m  = '"\x08B\x12\x05\x02\x02\xa3\x01\r\x00\x00\x00\xf9?\xc1\xfd`\x00\x08\x00\x06'"'
        try:

            for l in range(1, 101):
                channl["CH1" + "_SID_" + str(l)] = m
            for j in range(1, 101):
                channl["CH2" + "_SID_" + str(j)] = m

            # for l in range(1, 101):
            #     channl2["CH1" + "_SID_" + str(l)] = "0242120502029f0114000002173fd8955f000b000bc5b0"
            # for j in range(1, 101):
            #     channl2["CH2" + "_SID_" + str(j)] = "0242120502029f0114000002173fd8955f000b000bc5b0"

            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in channl.keys())
            value1 = ', '.join("'" + str(x).replace('/', '_') + "'" for x in channl.values())
            # value2 = ', '.join("'" + str(x).replace('/', '_') + "'" for x in channl2.values())
            tablename = "sensor_master_112"

            # values = [value1,value2]
            # print(values)
            #print(value1)
            #print(value2)
            #print(columns)

            # for i in range(100):
            #      start = time.time()
            sql = "INSERT INTO sensor_master_112 ( %s ) VALUES ( %s );" % (columns, value1)

            # execute the insert query
            cursor1.execute(sql)
            #      end = time.time()
            #      print(end - start)
            print(" success ",m)
        except mysql.connector.Error as err:
            data += m +'\n'
            print(m, " ------------------------FAIL ")
            print(err)
    print(data.encode())

# Parse the master.config file and store the sensor master's ID
def read_master_config(ip):
    # Global variables
    global master_id
    global db_ip
    global database_name
    global db_userid
    global db_password

    # Open the master.config file and store the contents
    f = open('master.config', 'r')
    conf = f.read()
    f.close()

    # Parse the contents of the file
    conf = json.loads(conf)
    master_id = conf["master_id"]
    print("Master ID:" + str(master_id))

    # Database host
    db_ip=ip
    # Database name
    database_name=conf["database_name"]
    # Database user ID
    db_userid=conf["user_id"]
    # Database password
    db_password=conf["password"]

#Function to check the connection with the database machine and to the particular database with the user id and password
def check_database_connection():
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active

    print("Verifying database connection...")

    try:
        # Attempt database connection
        connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
        print("Database connection is online...")
        # Set database connection flag to true
        database_active=True
    except mysql.connector.Error as err:
        print("Database connection is offline...")
        # Set database connection flag to false
        database_active = False
        # Display the error on console
        troubleshoot_error(err,"database")
    finally:
        # Close the connection
        connection.close()

#Function to create the table when it does not already exists. The table with the columns serial number, timestamp and all the sensors is created
def create_raw_database_table():
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active
    global tablename
    global sensor_map
    global column_name

    try:
        # Attempt database connection
        print('Attempting to connect to the database...')
        connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
        print("Connection to the database established successfully...")
        cursor = connection.cursor(prepared=True)
        # Create cursor object
        tablename = "Sensor_Master_" + str(ID)
        # Execute the query to check if the data table exists
        cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'""".format(tablename.replace('\'', '\'\'')))
        # Check the query result
        if cursor.fetchone()[0] == 1:
            # Data table found
            print("Data table exists. Tablename: " + tablename + "...")
            # Check the number of columns in the Data table
            cursor.execute("""SELECT count(*) FROM information_schema.columns WHERE table_name ='{0}'""".format(tablename.replace('\'', '\'\'')))
            db_number_of_columns = cursor.fetchone()[0]
            # Verify the number of columns
            print(db_number_of_columns)
            if (db_number_of_columns) == rnge2 + 6:
                print("Data table has the required columns[" + str(db_number_of_columns) + "]...")
                # return True
            else:
                print("Data table does not have the required columns[" + str(db_number_of_columns) + "]...")

        else:
            # Data table not found
            print("Data table does not exist...")
            cursor.execute("CREATE TABLE " +tablename+ "(serial INT AUTO_INCREMENT PRIMARY KEY, datatime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            print("Data table[" + tablename + "] has been created...")

            for i in range(rnge1, rnge2 + 1):
                column_name = ""
                column_name += ""
                # Column name string
                for j in range(1, 100 + 1):
                    # Add sensor Ids to the column name string
                    column_name+= "CH1_SID_" + str(j) + " VARBINARY(50), "
                for k in range(101,110+1):
                    column_name += "CH1_S_IO_" + str(k) + " VARBINARY(50), "
                for l in range(111, 112 + 1):
                    column_name += "CH1_SID_" + str(l) + " VARBINARY(50), "
                for j in range(1, 100 + 1):
                    # Add sensor Ids to the column name string
                    column_name += "CH2_SID_" + str(j) + " VARBINARY(50), "
                for k in range(101, 110 + 1):
                    column_name += "CH2_S_IO_" + str(k) + " VARBINARY(50), "
                for l in range(111, 112 + 1):
                    column_name += "CH2_SID_" + str(l) + " VARBINARY(50), "

            column_name = column_name[:-2]
            # Alter the data table and add the sensor id columns
            query = "ALTER TABLE {} ADD COLUMN ({})".format(tablename, column_name)
            cursor.execute(query)
            connection.commit()
            print("Data table[" + tablename + "] has been altered and columns were added")

    except mysql.connector.Error as err:
        print("Database connection failed...")
        # Set database connection flag to false
        database_active = False
        # Display the error on console
        troubleshoot_error(err,"database")
    finally:
        # Close the connection
        connection.close()

def create_Relay_database_table():
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active
    global tablename
    global sensor_map
    global column_name

    try:
        # Attempt database connection
        print('Attempting to connect to the database...')
        connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
        print("Connection to the database established successfully...")
        cursor = connection.cursor(prepared=True)
        # Create cursor object
        tablename = "Relay_board"
        # Execute the query to check if the data table exists
        cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'""".format(tablename.replace('\'', '\'\'')))
        # Check the query result
        if cursor.fetchone()[0] == 1:
            # Data table found
            print("Data table exists. Tablename: " + tablename + "...")
            # Check the number of columns in the Data table
            cursor.execute("""SELECT count(*) FROM information_schema.columns WHERE table_name ='{0}'""".format(tablename.replace('\'', '\'\'')))
            db_number_of_columns = cursor.fetchone()[0]
            # Verify the number of columns
            if (db_number_of_columns) == 14:
                print("Data table has the required columns[" + str(db_number_of_columns) + "]...")
                # return True
            else:
                print("Data table does not have the required columns[" + str(db_number_of_columns) + "]...")
                return False
        else:
            # Data table not found
            print("Data table does not exist...")
            cursor.execute("CREATE TABLE " +tablename+ "(serial INT AUTO_INCREMENT PRIMARY KEY, datatime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            print("Data table[" + tablename + "] has been created...")

            column_name = ""
            column_name += "MasterID" + " VARCHAR(10), "
            column_name+="R1" + " VARCHAR(10), "
            column_name+="R2" + " VARCHAR(10), "
            column_name+="R3" + " VARCHAR(10), "
            column_name+="R4" + " VARCHAR(10), "
            column_name+="EXTIN" + " VARCHAR(10), "
            column_name+="IP1" + " VARCHAR(10), "
            column_name+="IP2" + " VARCHAR(10), "
            column_name+="IP3" + " VARCHAR(10), "
            column_name+="IP4" + " VARCHAR(10), "
            column_name+="OP1" + " VARCHAR(10), "
            column_name+="OP2" + " VARCHAR(10), "
            column_name+="OP3" + " VARCHAR(10), "
            column_name+="OP4" + " VARCHAR(10), "

            column_name = column_name[:-2]
            print(column_name)
            # Alter the data table and add the sensor id columns
            query = "ALTER TABLE {} ADD COLUMN ({})".format(tablename, column_name)
            cursor.execute(query)
            # end = time.time()
            # print(end-start)
            connection.commit()
            print("Data table[" + tablename + "] has been altered and columns were added")

    except mysql.connector.Error as err:
        print("Database connection failed...")
        # Set database connection flag to false
        database_active = False
        # Display the error on console
        troubleshoot_error(err,"database")
    finally:
        # Close the connection
        connection.close()
        end = time.time()
        # print(str(end-start))

#Method to merge all the dictionaries to one to write to the database
def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

#Method to troubleshoot the errors occuring based on their type eg. during database Connection
def troubleshoot_error(err,err_type):
    global database_active

    print("\n----------------------------------------------------------------")
    print("###############################################################")
    print("----------------------------------------------------------------")
    print("Solve the error by following steps")
    print("----------------------------------------------------------------")
    print("Note the error code from the statements below:")
    print("Something went wrong: {}".format(err))
    print("----------------------------------------------------------------")

    if(err_type=="database"):
        print("Based on the error code, correct the database parameters in master.config file\nError Code\tSolution\n2003\t\tEnter correct ip address in master.config file\n1049\t\tEnter correct database name in master.config file\n1045\t\tEnter correct UserID and Password in master.config file")
        database_active = False

    print("\n----------------------------------------------------------------")
    print("###############################################################")
    print("----------------------------------------------------------------\n\n")

# Main function
def main():
    # Global variables
    global sensor_map
    global database_active

    # Read the master ID
    read_master_config("192.168.5.66")
    #read_master_config("192.168.5.126")
    # Check database connection
    check_database_connection()
    # create_Relay_database_table()
    # Create the database table if it does not exist
    create_raw_database_table()

    thread1 = Thread(target=poll_sensors, args=(["192.168.5.66"]))
    thread1.start()
    thread1.join()
    #poll_sensors()



# Start the main function
main()
