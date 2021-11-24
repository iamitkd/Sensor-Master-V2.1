import json
#from Queue import Queue
from threading import Thread
import serial
import asyncore
#import fcntl
import os.path
from os import path
import struct
import copy
import threading
import time
import mysql.connector
from mysql.connector import connect
from classes import configure
from classes import set
from classes import relay
import codecs

T_lock = threading.RLock()

# Sensor Channel dictionaries
sensor_map_4 = {}
sensor_map_1 = {}
sensor_map = {}

relay = {"R1": 0, "R2": 0, "R3": 0, "R4": 0, "EXTIN": 0, "IP1": 0, "IP2": 0, "IP3": 0, "IP4": 0, "OP1": 0, "OP2": 0, "OP3": 0, "OP4": 0}

# Default Sensor Master ID
master_id = -1

# import RS485 Serial Port Query Parameters
SLAVE_PKT_HDR = chr(2)
SLAVE_PKT_TAIL = chr(3)
CMD_QUERY_SLAVE_STATUS = chr(48)
#######################################################################################################################
#######################################################################################################################

# # Toggle GPIO output for audio-visual alarm triggers
def toggle_alarm_output(new_alarm_received):

    global master_id
    global relay
    # Database Parameters
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active

    # Check if a new alarm was received and create a new stopping time for the alarm by adding the alarm interval
    if (new_alarm_received):
        relay.alarm_stop_timer = time.time() + relay.alarm_interval
        alarm_gpio_status["R3"] = alarm_gpio_status["R4"] = 1;
        if alarm_gpio_status["R3"] == alarm_gpio_status["R4"] == 1 and relay.turn_off_alarms:
            print("Switching on GPIO Alarm Output")
            relay.alarm_status(True)

    relay.turn_off_alarms = False

    # Query and Update the sensor masters database with the latest GPIO Alarm Output status
    if database_active!=False:
        try:
            # Attempt database connection
            #print('Attempting to connect to the database...')
            connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
            #print("Connection to the database established successfully...")
            # Create cursor object
            cursor = connection.cursor(prepared=True)
            # Table name string format: M<master id>_<total sensors>_<sensor range start id>_<sensor range end id>
            tablename = "sensor_masters1"
            # Execute the query to check if the data table exists
            cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'""".format(tablename.replace('\'', '\'\'')))
            # Check the query result

            if cursor.fetchone()[0] == 1:
             # Update the alarm status if the a new alarm was received
                if (new_alarm_received):
                    # Update the sensor master data table with new data if there was a new alarm
                    cursor.execute("""UPDATE sensor_masters1 SET R1 = """ + """'""" + str(alarm_gpio_status["R1"]) + """'"""
                                                         + """, R2 = """ + """'""" + str(alarm_gpio_status["R2"]) + """'"""
                                                         + """, R3 = """ + """'""" + str(alarm_gpio_status["R3"]) + """'"""
                                                         + """, R4 = """ + """'""" + str(alarm_gpio_status["R4"]) + """'"""
                                                         + """, EXTIN = """ + """'""" + str(alarm_gpio_status["EXTIN"]) + """'"""
                                                         + """, IP1 = """ + """'""" + str(alarm_gpio_status["IP1"]) + """'"""
                                                         + """, IP2 = """ + """'""" + str(alarm_gpio_status["IP2"]) + """'"""
                                                         + """, IP3 = """ + """'""" + str(alarm_gpio_status["IP3"]) + """'"""
                                                         + """, IP4 = """ + """'""" + str(alarm_gpio_status["IP4"]) + """'"""
                                                         + """, OP1 = """ + """'""" + str(alarm_gpio_status["OP1"]) + """'"""
                                                         + """, OP2 = """ + """'""" + str(alarm_gpio_status["OP2"]) + """'"""
                                                         + """, OP3 = """ + """'""" + str(alarm_gpio_status["OP3"]) + """'"""
                                                         + """, OP4 = """ + """'""" + str(alarm_gpio_status["OP4"]) + """'"""
                                                         + """ WHERE (MasterId = '1')""")
                    connection.commit()

                # Query the sensor master database and wait for user intervention from VIGIL
                if not relay.turn_off_alarms:
                    # Query the sensor master data table
                    cursor.execute("""SELECT * FROM sensor_masters1 WHERE MasterId ='{0}'""".format(str(master_id)))
                    result=cursor.fetchone()
                    # Read and store the alarm output columns from the result of the query
                    if result[4] == result[5] == 0:
                        relay.turn_off_alarms = True

                # Update the sensor master data table with new data if there was a new alarm
                cursor.execute("""UPDATE sensor_masters1 SET R1 = """ + """'""" + str(alarm_gpio_status["R1"]) + """'"""
                                                     + """, R2 = """ + """'""" + str(alarm_gpio_status["R2"]) + """'"""
                                                     + """, R3 = """ + """'""" + str(alarm_gpio_status["R3"]) + """'"""
                                                     + """, R4 = """ + """'""" + str(alarm_gpio_status["R4"]) + """'"""
                                                     + """, EXTIN = """ + """'""" + str(alarm_gpio_status["EXTIN"]) + """'"""
                                                     + """, IP1 = """ + """'""" + str(alarm_gpio_status["IP1"]) + """'"""
                                                     + """, IP2 = """ + """'""" + str(alarm_gpio_status["IP2"]) + """'"""
                                                     + """, IP3 = """ + """'""" + str(alarm_gpio_status["IP3"]) + """'"""
                                                     + """, IP4 = """ + """'""" + str(alarm_gpio_status["IP4"]) + """'"""
                                                     + """, OP1 = """ + """'""" + str(alarm_gpio_status["OP1"]) + """'"""
                                                     + """, OP2 = """ + """'""" + str(alarm_gpio_status["OP2"]) + """'"""
                                                     + """, OP3 = """ + """'""" + str(alarm_gpio_status["OP3"]) + """'"""
                                                     + """, OP4 = """ + """'""" + str(alarm_gpio_status["OP4"]) + """'"""
                                                     + """ WHERE (MasterId = '""" + str(master_id) + """')""")
                connection.commit()
            else:
                # Data table not found
                print("Sensor Master Data table does have information about this sensor master...")
#
        except mysql.connector.Error as err:
            print("Database connection could not be verified...")
            # Set database connection flag to false
            database_active = False
            # Display the error on console
            troubleshoot_error(err,"database")
        finally:
            # Close the connection
            connection.close()
    else:
        print("Database is offline...")

    # Check if the alarm interval has passed by and no new alarms were received or user intervention
    if (relay.alarm_stop_timer <= time.time()):
        relay.turn_off_alarms = True

        if (relay.turn_off_alarms):
            alarm_gpio_status["R3"] = alarm_gpio_status["R4"] = 0;
            print("--Switching off GPIO Alarm Output")
            relay.alarm_status(False)

######################################################################################################################################

# Send query on the RS485 serial port
def send_rs485(tx_str, serial_obj):
    serial_obj.write(tx_str)
    serial_obj.flush()

# Looping threads for polling sensors on individual sensor channels
def poll_sensors(serial_obj, map_inp):

    # Global Variables
    global T_lock
    global sensor_map_1
    global sensor_map_4
    global sensor_map

    # Ignore List
    ignore_list = []

    # Poll every sensor in the dictionary mapped to the serial port and parse their responses
    for sensor_id in map_inp:

        hex_id = "%0.2X" % sensor_id
        # Create query string
        query_str = SLAVE_PKT_HDR + hex_id + CMD_QUERY_SLAVE_STATUS + SLAVE_PKT_TAIL
        # Send query on the serial port
        send_rs485(query_str,serial_obj)
        # Receive response on the serial port
        resp_str =  serial_obj.read(11)

        # Acquire Thread lock
        T_lock.acquire()
        # Check for valid response from the sensor
        if(len(resp_str) > 0):
            if(resp_str.find('N', 1, -2) >= 0):
                # Alarm condition
                map_inp[sensor_id] = set.VIBRATION_ALARM
                # Turn on the alarm output
                toggle_alarm_output(True)
            elif(resp_str.find('P', 1, -2) >= 0):
                # Normal condition
                map_inp[sensor_id] = set.NORMAL
            elif(resp_str.find('T', 1, -2) >= 0):
                # High temperature condition
                map_inp[sensor_id] = set.TEMPERATURE_ALARM
            else:
                # No response
                map_inp[sensor_id] = set.NO_RESPONSE
        else:
            map_inp[sensor_id] = set.NO_RESPONSE

        # Release thread lock
        T_lock.release()

    # Poll the sensors which are not included in the mapped file
    sm = copy.deepcopy(sensor_map)

    for sensor_id in sm:
        # Generate hex ID
        hex_id = "%0.2X" % sensor_id
        # Create query string
        query_str = SLAVE_PKT_HDR + hex_id + CMD_QUERY_SLAVE_STATUS + SLAVE_PKT_TAIL
        # Send query on the serial port
        send_rs485(query_str,serial_obj)
        # Receive response on the serial port
        resp_str = serial_obj.read(11)


        # Acquire Thread lock
        T_lock.acquire()
        # Check for valid response from the sensor
        if(len(resp_str) > 0):
            if(resp_str.find('N', 1, -2) >= 0):
                # Alarm condition
                map_inp[sensor_id] = set.VIBRATION_ALARM
                # Turn on the alarm output
                toggle_alarm_output(True)
            elif(resp_str.find('P', 1, -2) >= 0):
                # Normal condition
                map_inp[sensor_id] = set.NORMAL
            elif(resp_str.find('T', 1, -2) >= 0):
                # High temperature condition
                map_inp[sensor_id] = set.TEMPERATURE_ALARM
            else:
                # No response
                map_inp[sensor_id] = set.NO_RESPONSE
        else:
            map_inp[sensor_id] = set.NO_RESPONSE
            ignore_list.append(sensor_id)
        # Release thread lock
        T_lock.release()

    T_lock.acquire()
    for _ in ignore_list:
        del sensor_map[_]
    T_lock.release()

def poll_relayboard():
    x = serial.Serial('/dev/ttymxc2', 9600, timeout=0.05)
    while True:
        req = "\x0A\x02\x1F\x3F\x00\x04\x4F\x6A"
        x.write(req)
        # print("Sending")
        # print("Received Response")
        response = codecs.encode(x.read(20), "HEX").upper()
        # print(response)

        if response == "0A020137E27A":

            relay["IP4"] = 1;
            relay["IP3"] = relay["IP2"] = relay["IP1"] = 0;

        elif response == "0A02013BE27F":

            relay["IP3"] = 1;
            relay["IP1"] = relay["IP2"] = relay["IP4"] = 0;

        elif response == "0A020133E3B9":

            relay["IP3"] = relay["IP4"] = 1;
            relay["IP1"] = relay["IP2"] = 0;

        elif response == "0A02013D627D":

            relay["IP2"] = 1;
            relay["IP3"] = relay["IP1"] = relay["IP4"] = 0;

        elif response == "0A02013563BB":
            relay["IP1"] = relay["IP3"] = 1;
            relay["IP2"] = relay["IP4"] = 0;

        elif response == "0A02013963BE":
            relay["IP2"] = relay["IP3"] = 1;
            relay["IP1"] = relay["IP4"] = 0;

        elif response == "0A0201316278":
            relay["IP2"] = relay["IP3"] = relay["IP4"] = 1;
            relay["IP1"] = 0;

        elif response == "0A02013E227C":

            relay["IP1"] = 1;
            relay["IP3"] = relay["IP2"] = relay["IP4"] = 0;

        elif response == "0A02013623BA":

            relay["IP1"] = relay["IP4"] = 1;
            relay["IP2"] = relay["IP3"] = 0;

        elif response == "0A02013A23BF":

            relay["IP1"] = relay["IP3"] = 1;
            relay["IP4"] = relay["IP2"] = 0;

        elif response == "0A0201322279":

            relay["IP1"] = relay["IP3"] = relay["IP4"] = 1;
            relay["IP2"] = 0;

        elif response == "0A02013CA3BD":
            relay["IP1"] = relay["IP2"] = 1;
            relay["IP3"] = relay["IP4"] = 0;

        elif response == "0A020134A27B":

            relay["IP1"] = relay["IP2"] = relay["IP4"] = 1;
            relay["IP3"] = 0;

        elif response == "0A020138A27E":

            relay["IP1"] = relay["IP2"] = relay["IP3"] = 1;
            relay["IP4"] = 0;

        elif response == "0A020130A3B8":
            relay["IP1"] = relay["IP2"] = relay["IP3"] = relay[
                "IP4"] = 1;

        elif response == "0A02013FE3BC":

            relay["IP1"] = relay["IP2"] = relay["IP3"] = relay[
                "IP4"] = 0;

        time.sleep(0.02)
        req = "\x0A\x01\x0F\x9F\x00\x04\x0F\x88"
        x.write(req)
        # print("Sending")
        # print("Received Response")
        response = codecs.encode(x.read(20), "HEX").upper()
        # print(response)

        if response == "0A01019053C0":

            relay["OP1"] = relay["OP2"] = relay["OP3"] = relay[
                "OP4"] = 0;

        elif response == "0A0101919200":

            relay["OP1"] = 1;
            relay["OP2"] = relay["OP3"] = relay["OP4"] = 0;

        elif response == "0A010192D201":

            relay["OP2"] = 1;
            relay["OP1"] = relay["OP3"] = relay["OP4"] = 0;

        elif response == "0A01019313C1":

            relay["OP1"] = relay["OP2"] = 1;
            relay["OP3"] = relay["OP4"] = 0;

        elif response == "0A0101945203":

            relay["OP3"] = 1;
            relay["OP2"] = relay["OP1"] = relay["OP4"] = 0;

        elif response == "0A01019593C3":

            relay["OP1"] = relay["OP3"] = 1;
            relay["OP2"] = relay["OP4"] = 0;

        elif response == "0A010196D3C2":

            relay["OP1"] = relay["OP4"] = 0;
            relay["OP2"] = relay["OP3"] = 1;

        elif response == "0A0101971202":

            relay["OP4"] = 0;
            relay["OP1"] = relay["OP2"] = relay["OP3"] = 1;

        elif response == "0A0101985206":

            relay["OP4"] = 1;
            relay["OP1"] = relay["OP2"] = relay["OP3"] = 0;

        elif response == "0A01019993C6":

            relay["OP1"] = relay["OP4"] = 1;
            relay["OP2"] = relay["OP3"] = 0;

        elif response == "0A01019AD3C7":

            relay["OP1"] = relay["OP3"] = 0;
            relay["OP2"] = relay["OP4"] = 1;

        elif response == "0A01019B1207":

            relay["OP1"] = relay["OP2"] = relay["OP4"] = 1;
            relay["OP3"] = 0;

        elif response == "0A01019C53C5":

            relay["OP1"] = relay["OP2"] = 0;
            relay["OP3"] = relay["OP4"] = 1;

        elif response == "0A01019D9205":

            relay["OP1"] = relay["OP3"] = relay["OP4"] = 1;
            relay["OP2"] = 0;

        elif response == "0A01019ED204":

            relay["OP2"] = relay["OP3"] = relay["OP4"] = 1;
            relay["OP1"] = 0;

        elif response == "0A01019F13C4":

            relay["OP1"] = relay["OP2"] = relay["OP3"] = relay[
                "OP4"] = 1;

# Poll all sensor IDs on both channels to find out which sensor is connected on which serial port
def poll_and_send(threadname, var):
    # Global variables
    global sensor_map_4
    global sensor_map_1
    global master_id
    global database_active
    global relay

    # Initialize serial ports
    ser1 = serial.Serial('/dev/ttymxc4', 9600, timeout= set.WAIT_TIME)
    ser2 = serial.Serial('/dev/ttymxc1', 9600, timeout= set.WAIT_TIME)

    ignore_list = []

    # Poll all sensors on serial port 1 for 3 cycles
    for x in range(3):
        for _ in sensor_map:
            hex_id = "%0.2X" % _
            # Create query string
            query_str = SLAVE_PKT_HDR + hex_id + CMD_QUERY_SLAVE_STATUS + SLAVE_PKT_TAIL
            # Send query on the serial port
            send_rs485(query_str,ser1)
            # Receive response on the serial port
            resp_str = ser1.read(11)

            # Add sensor id to the serial port map 4 if a valid response is received
            T_lock.acquire()
            if (len(resp_str) > 0):
                sensor_map_4[_] = set.NORMAL
                if _ not in ignore_list:
                    ignore_list.append(_)
            T_lock.release()

    # Poll all sensors on serial port 2 for 3 cycles
    for x in range(3):
        for _ in sensor_map:
            hex_id = "%0.2X" % _
            query_str = SLAVE_PKT_HDR + hex_id + CMD_QUERY_SLAVE_STATUS + SLAVE_PKT_TAIL
            send_rs485(query_str,ser2)
            resp_str = ser2.read(11)
            # Add sensor id to the serial port map 1 if a valid response is received

            T_lock.acquire()
            if (len(resp_str) > 0):
                sensor_map_1[_] = set.NORMAL
                if _ not in ignore_list:
                    ignore_list.append(_)
            T_lock.release()

    for _ in ignore_list:
        del sensor_map[_]

    # Main polling logic which will start the threads to poll each serial ports with corresponding sensor IDs
    while True:

        # Create threads to run the polling function for each serial port
        thread_mxc4 = Thread(target = poll_sensors,args = (ser1,sensor_map_4))
        thread_mxc1 = Thread(target = poll_sensors,args = (ser2,sensor_map_1))

        thread_mxc4.setDaemon(True)
        thread_mxc1.setDaemon(True)

        # Start the threads
        thread_mxc1.start()
        thread_mxc4.start()

        thread_mxc1.join()
        thread_mxc4.join()

        # Print the serial port maps along with their statuses
    	# print "Serial Channel 1: "
    	# print sensor_map_1
    	# print "Serial Channel 2: "
    	# print sensor_map_4

# ######################################################################################################################################

 #Method to merge all the dictionaries to one to write to the database
def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

def send_data():

    while True:
        # Merge the two dictionaries into one for database update
        merge_dict = merge_two_dicts(sensor_map, merge_two_dicts(sensor_map_1, sensor_map_4))
        # Update the database with the latest update
        if database_active != False:

            insert_data_in_database(merge_dict)
            # Check the alarm status
        toggle_alarm_output(False)

########################################################################################################################
# Parse the sensor_config.json file and store the sensor IDs to be polled
def sensor_config():
    # Global variables
    global master_id
    global sensor_map

    # Open the json file and read the array of sensor IDs for the respective master ID
    with open('/home/root/sensor_config.json') as json_file:
        data = json.load(json_file)
        for p in data[str(master_id)]:
            sensor_map[p] = 0

# Parse the master.config file and store the sensor master's ID
def master_config():

    global master_id
    global db_ip
    global database_name
    global db_userid
    global db_password

    # parse settings from class Configure and master.config file

    x = configure('master.config')
    conf = x.parse()

    master_id = conf["master_id"]
    print("Master ID:" + str(master_id))
    # Database host
    db_ip = conf["database_host"]
    # Database name
    database_name = conf["database_name"]
    # Database user ID
    db_userid = conf["user_id"]
    # Database password
    db_password = conf["password"]

######################################################################################################################

# Power down the sensor channels
def power_down_sensor_channels():
    print "Powering down sensor channels..."
    relay["R1"] = relay["R2"] = 0;
    relay.power_status(False)
# Power up the sensor channels
def power_up_sensor_channels():

    print"Powering up sensor channels..."
    relay["R1"] = relay["R2"] = 1;
    relay.power_status(True)
# Power reset the sensor channels
def reset_sensor_channels():

    power_down_sensor_channels()
    print "Waiting for " + str(set.power_down_interval) + " seconds to power down..."
    time.sleep(set.power_down_interval)

    power_up_sensor_channels()
    print "Waiting for " + str(set.power_up_interval) + " seconds to power up..."
    time.sleep(set.power_up_interval)

#########################################################################################################################
#########################################################################################################################
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

#Function to check if the table with the tablename(consisting of masterid, number of sensors connected and start and end sesnorid) already exists in the database or not
def verify_sensor_master_database():
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active
    global master_id
    global sensor_map
    global tablename
    global db_number_of_columns

    # Check if the database is online
    if database_active!=False:
        try:
            # Attempt database connection
            print('Attempting to connect to the database...')
            connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
            print("Connection to the database established successfully...")
            # Create cursor object
            cursor = connection.cursor(prepared=True)
            # Table name string format: M<master id>_<total sensors>_<sensor range start id>_<sensor range end id>
            tablename = "M"+str(master_id)+"_"+str(len(sensor_map))+"_"+str(sensor_map.keys()[0])+"_"+str(sensor_map.keys()[-1])
            # Execute the query to check if the data table exists

            cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'""".format(tablename.replace('\'', '\'\'')))
            # Check the query result
            if cursor.fetchone()[0] == 1:
                # Data table found
                print("Data table exists. Tablename: " + tablename + "...")
                # Check the number of columns in the Data table
                cursor.execute("""SELECT count(*) FROM information_schema.columns WHERE table_name ='{0}'""".format(tablename.replace('\'', '\'\'')))
                db_number_of_columns=cursor.fetchone()[0]
                # Verify the number of columns
                if (db_number_of_columns-2)==len(sensor_map):
                    print("Data table has the required columns[" + str(db_number_of_columns) + "]...")
                    return True
                else:
                    print("Data table does not have the required columns[" + str(db_number_of_columns) + "]...")
                    return False
            else:
                # Data table not found
                print("Data table does not exist...")
                return False
        except mysql.connector.Error as err:
            print("Database connection could not be verified...")
            # Set database connection flag to false
            database_active = False
            # Display the error on console
            troubleshoot_error(err,"database")
        finally:
            # Close the connection
            connection.close()
    else:
        print("Database is offline...")

    # Default return value if the database operations fail
    return False

#Function to create the table when it does not already exists. The table with the columns serial number, timestamp and all the sensors is created
def create_database_table():
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global database_active
    global tablename
    global sensor_map
    global column_name

    # Check if the database is online
    if database_active!=False:
        try:
            # Attempt database connection
            print('Attempting to connect to the database...')
            connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
            print("Connection to the database established successfully...")
            # Create cursor object
            cursor = connection.cursor(prepared=True)
            # Create the data table in the database with the appropriate name
            cursor.execute("CREATE TABLE " +tablename+"(serial INT AUTO_INCREMENT PRIMARY KEY, datatime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            print("Data table[" + tablename + "] has been created...")
            # Column name string
            column_name=""
            # Add sensor Ids to the column name string
            for sen_id in sensor_map:
                column_name+="sensor"+str(sen_id)+" VARCHAR(100), "
            column_name=column_name[:-2]
            # Alter the data table and add the sensor id columns
            query = "ALTER TABLE {} ADD COLUMN ({})".format(tablename,column_name)
            cursor.execute(query)
            #call the commit method of the connection object. This method runs the SQL query in the database.
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
    else:
        print("Database is offline...")

# #Method to enter data in the table
def insert_data_in_database(merge_dict):
    # Global variables
    global db_ip
    global database_name
    global db_userid
    global db_password
    global tablename
    global database_active

    # Query building variables
    column_name=""
    column_data=""

    # Display the new status on the console
    print("Sensor status:")
    print(merge_dict)
    print("Alarm Output Status:")
    print(relay)

    # Check if the database is online
    if database_active!=False:
        try:
            # Attempt database connection
            connection = connect(host=db_ip,database=database_name,user=db_userid,password=db_password)
            # Create cursor object
            cursor = connection.cursor(prepared=True)
            # Create the insert query
            for sen_id in merge_dict:
                column_name+=sen_id+", "
                column_data+=str(merge_dict[sen_id])+", "
            column_name=column_name[:-2]
            column_data=column_data[:-2]
            # build the insert query
            sql_stmt = """INSERT INTO {} ({}) VALUES ({})""".format(tablename,column_name,column_data)
            #print("Insert query: " + sql_stmt)
            # execute the insert query
            cursor.execute(sql_stmt)
            connection.commit()

            print("New update has been pushed to the database...")
        except mysql.connector.Error as err:
            print("Database connection failed...")
            # Set database connection flag to false
            database_active = False
            # Display the error on console
            troubleshoot_error(err,"database")
        finally:
            # Close the connection
            connection.close()
    else:
        print("Database is offline...")

#########################################################################################################################
######################################################################################################################

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
        database_active=False
    print("\n----------------------------------------------------------------")
    print("###############################################################")
    print("----------------------------------------------------------------\n\n")

# #####################################################################################################################

# Main function
def main():
     # Global variables
    global sensor_map
    global database_active

     # Power reset the sensor channels
    reset_sensor_channels()

     # Read the master ID
    master_config()

     # Read the sensor configuration for the sensor master
    sensor_config()

    # Check database connection
    check_database_connection()

    # Verify if this sensor master's update data table exists in the database
    data_table_exists = verify_sensor_master_database()

    # Create the database table if it does not exist
    if not data_table_exists:
        create_database_table()

    queue = Queue()

    thread1 = Thread(target = poll_and_send, args = ("Polling Daemon Service", queue))
    thread2 = Thread(target = poll_relayboard)
    thread3 = Thread(target= send_data)

    thread1.setDaemon(True)
    thread2.setDaemon(True)
    thread3.setDaemon(True)

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()

    while True:
        asyncore.loop()

 #######################################################################################################################

# Start the main function
main()