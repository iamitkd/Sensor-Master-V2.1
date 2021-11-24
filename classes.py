from ConfigParser import SafeConfigParser
from periphery import GPIO
from threading import Thread
import mysql.connector
import serial
import json
import codecs
import struct
import time
import threading

config = SafeConfigParser()
config.read('/home/root/config.ini')

# Relay Channel default configurtion
ch_config = ('0','0')


# stop thread flags for runtime channel shift
stop_thread1 = False
stop_thread2 = False

flag_1 = "stop"
flag_2 = "stop"

# Sensor Channel dictionaries
ch_1_list = {}
ch_2_list = {}
channl_1 = {}
channl_2 = {}
channl = {}
sensor_config_json={}

# Sensor info lists 
sensID = []
sensUID = []
fuse_byte = []
setpoint = []
newsetpoint = []

relay = {"CH1_volts":0, "CH1_current":0, "CH2_volts":0, "CH2_current":0, "R1": 0, "R2": 0, "R3": 0, "R4": 0, "EXTIN": 0, "IP1": 0, "IP2": 0, "IP3": 0, "IP4": 0, "OP1": 0, "OP2": 0, "OP3": 0, "OP4": 0}

class relay_board:

    global ch1_relay
    global ch2_relay
    global alarm_1
    global alarm_2
    global turn_off_alarms

    alarm_stop_timer = config.getint('Relay_board','alarm_stop_timer')
    alarm_interval = config.getint('Relay_board','alarm_interval')
    turn_off_alarms = config.get('Relay_board','turn_off_alarms')
    ch1_relay = GPIO(config.getint('Relay_board','Channel_1'), 'out')
    ch2_relay = GPIO(config.getint('Relay_board','Channel_2'), 'out')
    alarm_1 = GPIO(config.getint('Relay_board','alarm_1'),'out')
    alarm_2 = GPIO(config.getint('Relay_board','alarm_2'), 'out')
    power_up = config.getint('Relay_board','power_up_interval')
    power_down = config.getint('Relay_board', 'power_down_interval')

    @staticmethod
    def power_status(a):
        ch1_relay.write(a)
        ch2_relay.write(a)

    @staticmethod
    def relay_status(x,a):
        x.write(a)
        x.write(a)

    @staticmethod
    def alarm_status(b):
        alarm_1.write(b)
        alarm_2.write(b)

class mysqldb:
    
    global ch_config

    master_id = config.get('Master', 'master_id')
    db_primary = config.get('Master', 'database_host_primary')
    db_secondary = config.get('Master', 'database_host_secondary')
    db_name = config.get('Master', 'database_name')
    db_userid = config.get('Master', 'user_id')
    db_password = config.get('Master', 'password')


    #######################################################################################################################

    
    # Toggle GPIO output for audio-visual alarm triggers
    @staticmethod
    def toggle_alarm(new_alarm_received):
        global alarm_1
        global alarm_2  
        global relay
        global db_alarm
        global turn_off_alarms
   
        # Check if a new alarm was received and create a new stopping time for the alarm by adding the alarm interval

        if new_alarm_received:
            relay_board.alarm_stop_timer = time.time() + relay_board.alarm_interval
            relay["R3"] = relay["R4"] = 1
            relay_board.alarm_status(True) 
            db_alarm = True             
 
        # if new alarm is not received wait for alarm interval to finish and then turn off the alarms

        turn_off_alarms = False
        if (relay_board.alarm_stop_timer <= time.time()):
            turn_off_alarms = True

        if turn_off_alarms:
            relay["R3"] = relay["R4"] = 0
            relay_board.alarm_status(False)

   
    @staticmethod
    def connect_db(a):

        """ Connect to MySQL database using Mysqlconnector"""

        conn = None
        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            if conn.is_connected():
                print('Connected to primary database')

        except mysql.connector.Error as err:
            print(err)
            print("No connection to primary database")

        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    @staticmethod
    def fetch_System_Mode(a):

        """ Fetch system mode from MySQL database """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT SystemMode FROM liminal_k_modbus.relay_config WHERE MasterId = " + str(mysqldb.master_id))

            row = cursor.fetchone()
            return row[0]

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    # Return channel config for runtime switching
    @staticmethod
    def get_relay_config():
        global ch_config
        return ch_config

    # Fetch channel config for runtime switching
    @staticmethod
    def fetch_relay_config(a):

        global ch1_relay
        global ch2_relay
        global ch_config
        global relay
        global db_alarm
        global turn_off_alarms

        while True:

            db_alarm = False

            """ fetch Relay config from Database and store in sensor master """

            try:
                conn = mysql.connector.connect(host=a,
                                               database=mysqldb.db_name,
                                               user=mysqldb.db_userid,
                                               password=mysqldb.db_password,autocommit=True)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT CH1,CH2 FROM liminal_k_modbus.relay_config WHERE MasterId = " + str(mysqldb.master_id))

                row = cursor.fetchone()
                for x in row:
                    
                    if row[0] == 'Q' and row[1] == 'Q':
                        relay_board.relay_status(ch1_relay, True)
                        relay_board.relay_status(ch2_relay, True)
                        ch_config = 'Q', 'Q'
                        relay["R1"] = relay ["R2"] = 1
                        mysqldb.get_relay_config()

                    elif row[0] == 'Q' and row[1] == 'L':
                        relay_board.relay_status(ch1_relay, True)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'Q', 'L'
                        relay["R1"] = 1
                        relay ["R2"] = 0
                        mysqldb.get_relay_config()

                    elif row[0] == 'L' and row[1] == 'Q':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, True)
                        ch_config = 'L', 'Q'
                        relay["R1"] = 0
                        relay ["R2"] = 1
                        mysqldb.get_relay_config()

                    elif row[0] == 'L' and row[1] == 'L':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'L', 'L'
                        relay["R1"] = relay ["R2"] = 0
                        mysqldb.get_relay_config()

                    elif row[0] == 'Q' and row[1] == 'X':
                        relay_board.relay_status(ch1_relay, True)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'Q', 'X'
                        relay["R1"] = 1
                        relay ["R2"] = 0
                        mysqldb.get_relay_config()

                    elif row[0] == 'L' and row[1] == 'X':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'L', 'X'
                        relay["R1"] = relay ["R2"] = 0
                        mysqldb.get_relay_config()

                    elif row[0] == 'X' and row[1] == 'Q':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, True)
                        ch_config = 'X', 'Q'
                        relay["R1"] = 0
                        relay ["R2"] = 1
                        mysqldb.get_relay_config()

                    elif row[0] == 'X' and row[1] == 'L':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'X', 'L'
                        relay["R1"] = relay ["R2"] = 0
                        mysqldb.get_relay_config()

                    elif row[0] == 'X' and row[1] == 'X':
                        relay_board.relay_status(ch1_relay, False)
                        relay_board.relay_status(ch2_relay, False)
                        ch_config = 'X', 'X'
                        relay["R1"] = relay ["R2"] = 0
                        mysqldb.get_relay_config()


                # update relay config in MYsql DB if a new alarm was recieved

                if db_alarm:
                    query = ("Update liminal_k_modbus.relay_config SET R3 = %s, R4 = %s, EXTIN = %s WHERE MasterId = " + str(mysqldb.master_id))
                    data = (relay["R3"],relay["R4"],relay["EXTIN"])
                    cursor.execute(query,data)


                # check for user intervention from Vigil software

                if turn_off_alarms == False:
                    cursor.execute("SELECT R3,R4 FROM liminal_k_modbus.relay_config WHERE MasterId = " + str(mysqldb.master_id))
                    row = cursor.fetchone()
                    if row[0] == row[1] == 0:
                        relay["R3"] = relay["R4"] = 0
                        relay_board.alarm_status(False)

                # update relay config in MYsql DB after switching off the relays

                query2 = ("Update liminal_k_modbus.relay_config SET R3 = %s, R4 = %s, EXTIN = %s WHERE MasterId = " + str(mysqldb.master_id))
                data2 = (relay["R3"],relay["R4"],relay["EXTIN"])
                cursor.execute(query2,data2)

            except mysql.connector.Error as err:
                print(err)

            finally:
                if conn is not None and conn.is_connected():
                    conn.close()

    @staticmethod
    def fetch_sensor_config(a):

        """ fetch sensor config from Database and store in sensor master """

        global sensor_config_json
        sensor_list_ch1 = []
        sensor_list_ch2 = []

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            cursor.execute("SELECT SensorId FROM liminal_k_modbus.sensor_config WHERE MasterId = " +str(mysqldb.master_id))
            Querry = cursor.fetchall()
            for x in Querry:
                sensor_list_ch1.append(x[0])

            cursor.execute("SELECT SensorId FROM liminal_k_modbus.sensor_config WHERE ListnerId = " +str(mysqldb.master_id))
            listen = cursor.fetchall()
            for x in listen:
                sensor_list_ch2.append(x[0])
            sensor_config_json["1"] = sensor_list_ch1
            sensor_config_json["2"] = sensor_list_ch2
            #print(sensor_config_json)
            
            with open("sensor.config.json", "w") as outfile:
                json.dump(sensor_config_json, outfile)
        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    # convert float to hex
    @staticmethod
    def float_to_hex(a):
        return hex(struct.unpack('<I', struct.pack('<f', a))[0])


    @staticmethod
    def fetch_Setpoint(a,m,channel):

        """ fetch new sensor setpoint from Database and store in sensor master  """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            query = "SELECT SensorId,StdrSetpoint,RstdrSetpoint,MaxRateSetpoint FROM liminal_k_modbus.sensor_config WHERE MasterId = %s and ChannelId = %s"
            data = (str(mysqldb.master_id),m)
            cursor.execute(query,data)
            Querry = cursor.fetchall()

            #EEProm requires 3.5 ms to write 1 byte threrefore this disturbs the algorithm if multipe setponts are written together.

            for x in Querry:
                if x[0] != 111 and x[0] != 112:
                    id = int(str(x[0]).zfill(2))

                    if 0 < int(float(x[1])) < 21:
                        stdr = mysqldb.float_to_hex(float(x[1]))[2:]
                        tx = poll.makeByteBuff(poll.write_stdr((format(id, '02X')),stdr))
                        print(codecs.encode(tx, "HEX"))
                        crcdata = poll.Modbus_serial(channel,tx,13)
                        recvdata = str(codecs.encode(crcdata, "HEX"))
                        print(recvdata)
                    else:
                        print("Please Enter a Valid Stdr Setpoint")

                    if 2 < int(float(x[2])) < 31:
                        rstdr = mysqldb.float_to_hex(float(x[2]))[2:]
                        tx = poll.makeByteBuff(poll.write_rstdr((format(id, '02X')),rstdr))
                        print(codecs.encode(tx, "HEX"))
                        crcdata = poll.Modbus_serial(channel,tx,13)
                        recvdata = str(codecs.encode(crcdata, "HEX"))
                        print(recvdata)
                    else:
                        print("Please Enter a Valid RStdr Setpoint")

                    if 9 < int(x[3]) < 101:
                        maxrate = str(hex(int(x[3])))[2:].zfill(4)
                        tx = poll.makeByteBuff(poll.write_maxrate((format(id, '02X')),maxrate))
                        print(codecs.encode(tx, "HEX"))
                        crcdata = poll.Modbus_serial(channel,tx,13)
                        recvdata = str(codecs.encode(crcdata, "HEX"))
                        print(recvdata)
                    else:
                        print("Please Enter a Valid maxrate Setpoint")

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    @staticmethod
    def fetch_both_channel_sensor_config(a):

        """ fetch sensor config based on channels from Database and store in sensor master  """

        global sensor_config_json
        sensor_list_ch1 = []
        sensor_list_ch2 = []

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()

            query = "SELECT SensorId FROM liminal_k_modbus.sensor_config WHERE MasterId = %s and ChannelId = %s"
            data = (str(mysqldb.master_id),1)
            cursor.execute(query, data)
            Querry = cursor.fetchall()
            for x in Querry:
                sensor_list_ch1.append(x[0])

            query2 = "SELECT SensorId FROM liminal_k_modbus.sensor_config WHERE MasterId = %s and ChannelId = %s"
            data2 = (str(mysqldb.master_id),2)
            cursor.execute(query2, data2)
            listen = cursor.fetchall()
            for x in listen:
                sensor_list_ch2.append(x[0])
            sensor_config_json["1"] = sensor_list_ch1
            sensor_config_json["2"] = sensor_list_ch2
            print(sensor_config_json)

            with open("sensor.config.json", "w") as outfile:
                json.dump(sensor_config_json, outfile)
        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    @staticmethod
    def push_UID_to_Database(a,m):
   
        """ Fetch sensor UID from sensor Master & store sensor UID to Database  """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            for n in range(len(sensID)):
                query = "UPDATE sensor_config set UID = %s WHERE MasterId = %s and ChannelId = %s and SensorId = %s"
                data = (sensUID[n],str(mysqldb.master_id),m,sensID[n])
                cursor.execute(query, data)
                #conn.commit()
                print("UID inserted successfully into vibration_sensors table")

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    @staticmethod
    def push_Fuse_byte_to_Database(a, m):

        """ Fetch Fuse Bytes from sensor Master & store sensor UID to Database  """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            for n in range(len(sensID)):
                #print(sensID[n])
                if fuse_byte[n] == "0":
                    i1,i2,i3,i4,i5 = ('0','0','0','0','0')
                else:
                    #print(fuse_byte[n])
                    i1,i2,i3,i4,i5 = fuse_byte[n]
                query = "UPDATE sensor_config set APversion = %s, LowFuse = %s, HighFuse = %s, ExtendedFuse = %s, LockFuse = %s WHERE MasterId = %s and ChannelId = %s and SensorId = %s"
                #print(query)
                data = (i1,i2,i3,i4,i5,str(mysqldb.master_id),m,sensID[n])
                cursor.execute(query, data)
                #conn.commit()
                print("fuse Byte inserted successfully into vibration_sensors table")

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    @staticmethod
    def push_setpoint_to_Database(a, m):

        """ Push sensor setpoints to Database """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            for n in range(len(sensID)):
                #print(sensID[n])
                if setpoint[n] == "0":
                    i1,i2,i3 = ('0','0','0')
                else:
                    i1,i2,i3 = setpoint[n]
                query = "UPDATE sensor_config set StdrValue = %s, RstdrValue = %s, MaxRate = %s WHERE MasterId = %s and ChannelId = %s and SensorId = %s"
                #print(query)
                data = (i1,i2,i3,str(mysqldb.master_id),m,sensID[n])
                cursor.execute(query, data)
                #conn.commit()
                print("setpoint inserted successfully into vibration_sensors table")

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()


    @staticmethod
    def push_rawdata_to_Database(a,dict):

        """ Push Rawdata to Database """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
            table_name = "Sensor_Master_" + str(mysqldb.master_id)

            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (table_name, columns, values)       
            cursor.execute(sql)

            query2 = "UPDATE relay_config set CH1_volts = %s, CH1_current = %s, CH2_volts = %s, CH2_current = %s, R1 = %s, R2 = %s WHERE MasterId = %s"
            data2 = (relay['CH1_volts'],relay['CH1_current'],relay['CH2_volts'],relay['CH2_current'],relay['R1'],relay['R2'],str(mysqldb.master_id))
            cursor.execute(query2, data2)

        except mysql.connector.Error as err:
            print(err)
            print(dict)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    @staticmethod
    def verify_raw_Database(a):


        """ verify Rawdata table if present in DB """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            table_name = "Sensor_Master_" + str(mysqldb.master_id)
            query = ("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'""".format(table_name.replace('\'', '\'\'')))
            # Execute the query to check if the data table exists
            cursor.execute(query)
            if cursor.fetchone()[0] == 1:
                # Data table found
                print("Data table exists. Tablename: " + table_name + "...")
                # Check the number of columns in the Data table
                query2 = ("""SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{0}'""".format(table_name.replace('\'', '\'\'')))
                cursor.execute(query2)
                db_number_of_columns = cursor.fetchone()[0]
                # Verify the number of columns
                print(db_number_of_columns)
                if (db_number_of_columns) == 226:
                    print("Data table has the required columns[" + str(db_number_of_columns) + "]...")
                    return True
                else:
                    print("Data table does not have the required columns[" + str(db_number_of_columns) + "]...")
                    return False
            else:
                # Data table not found
                print("Data table does not exist")
                return False

        except mysql.connector.Error as err:
            print(err)
            return False

        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    @staticmethod
    def create_raw_Database(a):

        """ create Rawdata table if not present in DB """

        try:
            conn = mysql.connector.connect(host=a,
                                           database=mysqldb.db_name,
                                           user=mysqldb.db_userid,
                                           password=mysqldb.db_password,autocommit=True)
            cursor = conn.cursor()
            table_name = "Sensor_Master_" + str(mysqldb.master_id)
            query ="CREATE TABLE " + table_name + "(serial INT AUTO_INCREMENT PRIMARY KEY, datatime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            cursor.execute(query)
            print("Data table[" + table_name + "] has been created...")

            for i in range(1, 220):
                column_name = ""
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
            query = "ALTER TABLE {} ADD COLUMN ({})".format(table_name, column_name)
            cursor.execute(query)
            #conn.commit()
            print("Data table[" + table_name + "] has been altered and columns were added")

        except mysql.connector.Error as err:
            print(err)

        finally:
            if conn is not None and conn.is_connected():
                conn.close()

class poll:
    
    channel_1 = config.get('Serial', 'channel_1')
    channel_2 = config.get('Serial', 'channel_2')
    Baudrate = config.get('Serial', 'Baudrate')
    bytesize = config.getint('Serial', 'bytesize')
    parity = config.get('Serial', 'parity')
    stopbits = config.getint('Serial', 'stopbits')
    
    @staticmethod
    def time_out(n,lngth):
        t = ((n)*11.0/int(poll.Baudrate)) + 0.0018 + lngth*11.0/int(poll.Baudrate)
        return(t)

    @staticmethod
    def Modbus_serial(channel,tx_str,n):  # function to Write and Receive data pass(channel_details,tx_str,number_to_read) 
        global x
        lngth = len(tx_str)
        x = serial.Serial(port=channel, baudrate=poll.Baudrate, bytesize=poll.bytesize, parity=poll.parity, stopbits=poll.stopbits, timeout=0.03)
        #print("Serial port opened:" + channel)
        x.write(tx_str)
        recvdata = x.read(n)
        return recvdata
  
    @staticmethod
    def crc16(ptrToArray):  # //A standard CRC algorithm
        out = 0xffff
        carry = 0
        inputSize = len(ptrToArray)
        inputSize += 1
        for l in range(0, inputSize - 1):
            out ^= ptrToArray[l]
            for n in range(0, 8):
                carry = out & 1
                out >>= 1
                if (carry):
                    out ^= 0xA001
        return out

    @staticmethod
    def makeByteBuff(string):  # returns tx buffer after adding crc to byte array
        # string with encoding 'utf-8'
        # print("string length -> " + str(len(string)))
        arr = bytearray(string.upper())
        for i in range(0, len(arr)):
            #print(arr[i])
            if arr[i] <= 57:
                arr[i] = arr[i] - 48
            elif arr[i] >= 65 and arr[i] <= 70:
                arr[i] = arr[i] - 55
        pos = 0
        for i in range(0, len(arr)):
            arr[i] = arr[pos] << 4 | arr[pos + 1]
            if pos + 2 == len(arr):
                pos = i + 1
                break
            pos += 2
        # if pos == 2:
        # pos = 4
        crc = poll.crc16(arr[:pos])
        arr[pos] = crc % 256
        arr[pos + 1] = crc // 256
        #    print("Data length -> " + str(len(arr[:pos + 2])))
        #print("Data With CRC -> "),
        #print(codecs.encode(arr[:pos + 2], "HEX"))
        return arr[:pos + 2]

    @staticmethod
    def UIDfunction(sens_id):     # function to Read UID data pass(sensor_id)
        id = sens_id
        funcode = "11"
        return (id + funcode)
 

    @staticmethod
    def rec_process(res):     # function to Read UID data pass(sensor_id)
        x = res[:-2]
        response = x.replace('\x5c','\x5c\x5c')
        response = x.replace('\x27','\x5c\x27')
        response += '\\\x27' 
        return response
        
    @staticmethod
    def fuse_byte_function(sens_id):      # function to Read fuse data pass(sensor_id)
        id = sens_id
        funcode = "03"
        address = "ff00"
        quantreg = "0008"
        return (id + funcode + address + quantreg)


    @staticmethod
    def setpoint(sens_id):      # function to Read fuse data pass(sensor_id)
        id = sens_id
        funcode = "03"
        address = "8400"
        quantreg = "0005"
        return (id + funcode + address + quantreg)

    @staticmethod
    def write_stdr(sens_id,sp):
        id = sens_id
        funcode = "10"
        address = "8400"
        quantreg = "0002"
        noofby = "04"
        data = sp
        return (id + funcode + address + quantreg + noofby + data)

    @staticmethod
    def write_rstdr(sens_id,sp):
        id = sens_id
        funcode = "10"
        address = "8402"
        quantreg = "0002"
        noofby = "04"
        data = sp
        return (id + funcode + address + quantreg + noofby + data)

    @staticmethod
    def write_maxrate(sens_id,sp):
        id = sens_id
        funcode = "10"
        address = "8404"
        quantreg = "0001"
        noofby = "02"
        data = sp
        return (id + funcode + address + quantreg + noofby + data)

    @staticmethod
    def read_sensor_config():             # function to Read fuse data pass(sensor_id)

        global ch_1_list
        global ch_2_list
        # Open the json file and read the array of sensor IDs for the respective master ID
        with open('sensor.config.json') as json_file:
            data = json.load(json_file)
        for n in data["1"]:
            ch_1_list[n] = 0
        for n in data["2"]:
            ch_2_list[n] = 0
        print(ch_1_list)
        print(ch_2_list)

    @staticmethod
    def crc_check(a):
        buff = a[len(a) - 2:len(a)]
        intbuff = int(ord(buff[1]))<<8 | ord(buff[0])
        #print(intbuff )
        #print(type(intbuff))
        bytebuff = poll.crc16(bytearray(a[:(len(a) - 2)]))
        #print(bytebuff)

        if bytebuff == intbuff :
            return True
        else:
            return False

    @staticmethod
    def readUID(ch_list,channel,a,channelID):

        global sensID
        global sensUID

        for n in ch_list.keys():
            tx = poll.makeByteBuff(poll.UIDfunction((format(n, '02X'))))  # change function here
            print(codecs.encode(tx, "HEX"))
            lngth = len(tx)
            crcdata = poll.Modbus_serial(channel,tx,17)
            recvdata = str(codecs.encode(crcdata, "HEX"))
            #print(crcdata)
            print(recvdata)
            if len(crcdata) == 15:
                #print("Response from App mode")
                if poll.crc_check(crcdata) == True:
                    result = (recvdata[6:(len(recvdata) - 4)]).upper()
                    sensUID.append(result)
                    sensID.append(n)
                else:
                    print("CRC not matching")
                    sensID.append(n)
                    sensUID.append("0")
            elif len(crcdata) == 17:
                if poll.crc_check(crcdata) == True:
                    result = (recvdata[8:(len(recvdata) - 9)]).upper()
                    sensUID.append(result)
                    sensID.append(n)
                else:
                    print("CRC not matching")
                    sensID.append(n)
                    sensUID.append("0")
            elif crcdata == "":
                sensID.append(n)
                sensUID.append("0") 

        print(sensID)
        print(sensUID)   
        mysqldb.push_UID_to_Database(a,channelID)                   
        del sensID [:]
        del sensUID [:]
    
    @staticmethod
    def read_fuse_byte(ch_list,channel,a,channelID):
		
        global fuse_byte
        global sensID

        for n in ch_list.keys():
            tx = poll.makeByteBuff(poll.fuse_byte_function((format(n, '02X'))))  # change function here
            #print(tx)
            lngth = len(tx)
            #print(codecs.encode(tx, "HEX"))
            crcdata = poll.Modbus_serial(channel, tx, 23)
            recvdata = str(codecs.encode(crcdata, "HEX"))
            #print(crcdata)
            print(recvdata)
            if crcdata == "":
                result = "0"
                fuse_byte.append(result)
                sensID.append(n)
            elif poll.crc_check(crcdata) == True:
                result = str(recvdata[13]) + str(recvdata[17]) + str(recvdata[21]),str(recvdata[24:26]),str(recvdata[28:30]),str(recvdata[32:34]),str(recvdata[36:38])
                print(result)
                fuse_byte.append(result)
                sensID.append(n)               
            else:
                #print("CRC not matching")
                fuse_byte.append("0")
                sensID.append(n)
        mysqldb.push_Fuse_byte_to_Database(a, channelID)
        del fuse_byte [:]
        del sensID [:]


    @staticmethod
    def read_setpoint(ch_list, channel, a, channelID):

        global setpoint
        global sensID

        for n in ch_list.keys():
            if n != 111 and n != 112:
                tx = poll.makeByteBuff(poll.setpoint((format(n, '02X'))))  # change function here
                # print(tx)
                #print(codecs.encode(tx, "HEX"))
                crcdata = poll.Modbus_serial(channel, tx, 15)
                recvdata = str(codecs.encode(crcdata, "HEX"))
                # print(crcdata)
                print(recvdata)
                if crcdata == "":
                    result = "0"
                    setpoint.append(result)
                    sensID.append(n)
                elif poll.crc_check(crcdata) == True:
                    result = str(struct.unpack('!f',recvdata[6:14].decode('hex'))[0]),str(struct.unpack('!f',str(recvdata[14:22]).decode('hex'))[0]),int(recvdata[22:26],16)
                    print(result)
                    setpoint.append(result)
                    sensID.append(n)
                else:
                    # print("CRC not matching")
                    setpoint.append("0")
                    sensID.append(n)
        mysqldb.push_setpoint_to_Database(a, channelID)
        del setpoint[:]
        del sensID[:]

#######################################################################################################

    @staticmethod
    def Modbus_serial_read_rawdata(n):  # function to Write and Receive data
        global x
        x.flush()
        revdata = x.read(n)
        return revdata

########################################################################################################

    @staticmethod
    def cmnd_string(ch_list):
        #print(ch_list.keys())
        string = ""
        for i in range(1, max(ch_list.keys()) + 1):
            if i not in ch_list:
                # print("0")
                string += "0"
            else:
                # print("1")
                string += "1"
        #print(string[::-1])
        string1 = (int(string[::-1], 2))
        string2 = (format(string1,'0032X'))
        return string2

#######################################################################################################

    #######################################################################################################
    @staticmethod
    def poll_relayboard():

        global relay

        rb = serial.Serial('/dev/ttymxc2', 9600, timeout=0.05)
        gpio_out6 = GPIO(26, "in")

        while True:

            Val = gpio_out6.read()

            if Val == True:
                relay["EXTIN"] = 1
               # mysqldb.toggle_alarm(True)

            elif Val == False:
                relay["EXTIN"] = 0

            req = "\x0A\x02\x1F\x3F\x00\x04\x4F\x6A"
            rb.write(req)
            response = codecs.encode(rb.read(20), "HEX").upper()

            if response == "0A020137E27A":

                relay["IP4"] = 1;
                # relay["IP3"] = relay["IP2"] = relay["IP1"] = 0;

            elif response == "0A02013BE27F":

                relay["IP3"] = 1;
                # alarm_gpio_status["IP1"] = alarm_gpio_status["IP2"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A020133E3B9":

                relay["IP3"] = relay["IP4"] = 1;
                # alarm_gpio_status["IP1"] = alarm_gpio_status["IP2"] = 0;

            elif response == "0A02013D627D":

                relay["IP2"] = 1;
                # alarm_gpio_status["IP3"] = alarm_gpio_status["IP1"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A02013563BB":
                relay["IP1"] = relay["IP3"] = 1;
                # alarm_gpio_status["IP2"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A02013963BE":
                relay["IP2"] = relay["IP3"] = 1;
                # alarm_gpio_status["IP1"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A0201316278":
                relay["IP2"] = relay["IP3"] = relay["IP4"] = 1;
                # alarm_gpio_status["IP1"] = 0;

            elif response == "0A02013E227C":

                relay["IP1"] = 1;
                # alarm_gpio_status["IP3"] = alarm_gpio_status["IP2"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A02013623BA":

                relay["IP1"] = relay["IP4"] = 1;
                # alarm_gpio_status["IP2"] = alarm_gpio_status["IP3"] = 0;

            elif response == "0A02013A23BF":

                relay["IP1"] = relay["IP3"] = 1;
                # alarm_gpio_status["IP4"] = alarm_gpio_status["IP2"] = 0;

            elif response == "0A0201322279":

                relay["IP1"] = relay["IP3"] = relay["IP4"] = 1;
                # alarm_gpio_status["IP2"] = 0;

            elif response == "0A02013CA3BD":
                relay["IP1"] = relay["IP2"] = 1;
                # alarm_gpio_status["IP3"] = alarm_gpio_status["IP4"] = 0;

            elif response == "0A020134A27B":

                relay["IP1"] = relay["IP2"] = relay["IP4"] = 1;
                # alarm_gpio_status["IP3"] = 0;

            elif response == "0A020138A27E":

                relay["IP1"] = relay["IP2"] = relay["IP3"] = 1;
                # alarm_gpio_status["IP4"] = 0;

            elif response == "0A020130A3B8":
                relay["IP1"] = relay["IP2"] = relay["IP3"] = relay[
                    "IP4"] = 1;

            elif response == "0A02013FE3BC":

                relay["IP1"] = relay["IP2"] = relay["IP3"] = relay[
                    "IP4"] = 0;

 
    @staticmethod
    def Querry_channel_1(stop):

        global stop_thread1
        global stop_thread2
        global flag_1

        regstartAddr = 0x8500
        regQty = 9
        startSID = 1
        endSID = 112
        ch1_ids = len(ch_1_list.keys())
        print(ch1_ids)
        if ch1_ids <= 7:
            interval1 = 255
        else:
            interval1 = int(float(0.512 / ch1_ids) // .0003125)
        print(interval1)

        commandString_ch1 = format(0, '02X') + format(0x42, '02X') + format(regstartAddr, '04X') + format(
            regQty,
            '02X') + format(
            interval1, '02X') + format(startSID, '02X') + format(endSID, '02X') + format(ch1_ids, '02X')
        commandString_ch1 += poll.cmnd_string(ch_1_list)
        tx_1 = poll.makeByteBuff(commandString_ch1)
        print(codecs.encode(tx_1, "HEX"))

        lngth = len(tx_1)
        timeout1 = poll.time_out(23, lngth) + (interval1 * 0.0003125)
        print(timeout1)

        x1 = serial.Serial(port=poll.channel_1, baudrate=poll.Baudrate, bytesize=poll.bytesize,
                           parity=poll.parity,
                           stopbits=poll.stopbits, timeout=timeout1)

        x1.flush()
        x1.write(tx_1)
        time.sleep(0.50)

        # Poll every sensor in the channel 1 dictionary mapped to the serial port 1 and parse their responses
        for sensor_id in ch_1_list:
            res = x1.read(23)
            sens = codecs.encode(res, "HEX")

            if len(res) == 23 and poll.crc_check(res) == True:
                if (int(ord(res[0]))) == 111:
                    relay["CH1_volts"] = str((int(ord(res[5])) << 8 | int(ord(res[6]))) * 0.004)
                    if (int(ord(res[9])) << 8 | int(ord(res[10]))) > 0X7FFF:
                        current = (((int(ord(res[9])) << 8 | int(ord(res[10]))) - 0XFFFF) / 10.0)
                        relay["CH1_current"] = str(current)
                    else:
                        relay["CH1_current"] = str((int(ord(res[9])) << 8 | int(ord(res[10]))) / 10.0)

                print("sensor---------", (int(ord(res[0]))), codecs.encode(res, "HEX"))

            elif len(res) == 0:
                print("-----No response------")


        while True:

            x1.flush()
            time.sleep(0.00175)
            x1.write(tx_1)
            time.sleep(0.50)
            flag_1 = "pause"

            # Poll every sensor in the channel 1 dictionary mapped to the serial port 1 and parse their responses   
       
            for sensor_id in ch_1_list:
                res = x1.read(23)
                sens = codecs.encode(res, "HEX")
                
                if len(res) == 23 and poll.crc_check(res) == True:       

                    if (int(ord(res[0]))) == 111:
                        relay["CH1_volts"] = str((int(ord(res[5])) << 8 | int(ord(res[6]))) * 0.004)
                        if (int(ord(res[9])) << 8 | int(ord(res[10]))) > 0X7FFF:
                            current = (((int(ord(res[9])) << 8 | int(ord(res[10]))) - 0XFFFF) / 10.00)
                            relay["CH1_current"] = str(current)
                        else:
                            relay["CH1_current"] = str((int(ord(res[9])) << 8 | int(ord(res[10]))) / 10.00)

                    print("sensor---------", (int(ord(res[0]))), poll.rec_process(res))
                    channl_1["CH1_SID_" + str(int(ord(res[0])))] = poll.rec_process(res)

                    if sens[19:23] != "0000":
                        if int(ord(res[0])) != 111 and int(ord(res[0])) != 112:
                            mysqldb.toggle_alarm(True)
                            print("intrusion")
                    else:
                        pass

                elif len(res) == 0:
                    print("-----No response------")

                    #flag_1 = "stop"

            if mysqldb.get_relay_config()[0] != "Q":
                    stop_thread1 = True
                    stop_thread2 = True

            if stop():
                ("Thread killed")
                break
    
            flag_1 = "push"

    @staticmethod
    def Querry_channel_2(stop):

        global stop_thread1
        global stop_thread2
        global flag_2

        regstartAddr = 0x8500
        regQty = 9
        startSID = 1
        endSID = 112
        ch2_ids = len(ch_2_list.keys())
        if ch2_ids <= 7:
            interval2 = 255
        else:
            interval2 = int(float(0.512 / ch2_ids) // .0003125)
        print(interval2)

        commandString_ch2 = format(0, '02X') + format(0x42, '02X') + format(regstartAddr, '04X') + format(
            regQty,
            '02X') + format(interval2, '02X') + format(startSID, '02X') + format(endSID, '02X') + format(
            ch2_ids, '02X')
        commandString_ch2 += poll.cmnd_string(ch_2_list)

        tx_2 = poll.makeByteBuff(commandString_ch2)
        print(codecs.encode(tx_2, "HEX"))

        lngth = len(tx_2)
        timeout2 = poll.time_out(23, lngth) + (interval2 * 0.0003125)
        print(timeout2)

        x2 = serial.Serial(port=poll.channel_2, baudrate=poll.Baudrate, bytesize=poll.bytesize,
                           parity=poll.parity,
                           stopbits=poll.stopbits, timeout=timeout2)

        x2.flush()
        x2.write(tx_2)
        time.sleep(0.50)

        for sensor_id in ch_2_list:
            res = x2.read(23)
            sens = codecs.encode(res, "HEX")

            if len(res) == 23 and poll.crc_check(res) == True:

                if (int(ord(res[0]))) == 112:
                    relay["CH2_volts"] = str((int(ord(res[5])) << 8 | int(ord(res[6]))) * 0.004)
                    if (int(ord(res[9])) << 8 | int(ord(res[10]))) > 0X7FFF:
                        current = (((int(ord(res[9])) << 8 | int(ord(res[10]))) - 0XFFFF) / 10.00)
                        relay["CH2_current"] = str(current)
                    else:
                        relay["CH2_current"] = str((int(ord(res[9])) << 8 | int(ord(res[10]))) / 10.00)

                print("sensor---------", (int(ord(res[0]))), codecs.encode(res, "HEX"))
 
            elif len(res) == 0:
                print("-----No response------")

        while True:
         
            x2.flush()
            time.sleep(0.00175)
            x2.write(tx_2)
            time.sleep(0.50)
            flag_2 = "pause" 

            # Poll every sensor in the channel 1 dictionary mapped to the serial port 1 and parse their responses
            for sensor_id in ch_2_list:
                res = x2.read(23)
                sens = codecs.encode(res, "HEX")

                if len(res) == 23 and poll.crc_check(res) == True:

                    if (int(ord(res[0]))) == 112:
                        relay["CH2_volts"] = str((int(ord(res[5])) << 8 | int(ord(res[6]))) * 0.004)
                        if (int(ord(res[9])) << 8 | int(ord(res[10]))) > 0X7FFF:
                            current = (((int(ord(res[9])) << 8 | int(ord(res[10]))) - 0XFFFF) / 10.0)
                            relay["CH2_current"] = str(current)
                        else:
                            relay["CH2_current"] = str((int(ord(res[9])) << 8 | int(ord(res[10]))) / 10.0)

                    print("sensor---------", (int(ord(res[0]))), poll.rec_process(res)) 
                    channl_2["CH2_SID_" + str(int(ord(res[0])))] = poll.rec_process(res)
                    if sens[19:23] != "0000":
                        if int(ord(res[0])) != 111 and int(ord(res[0])) != 112:
                            mysqldb.toggle_alarm(True)
                            print("intrusion")
                    else:
                        pass

                elif len(res) == 0:
                    print("-----No response------")

                    #flag_2 = "stop"

            if mysqldb.get_relay_config()[1] != "Q":
                stop_thread1 = True
                stop_thread2 = True

            if stop():
                ("Thread killed")
                break
                
            flag_2 = "push"


    @staticmethod
    def Listen_channel_1(stop):

        global stop_thread2
        global stop_thread1
        global flag_1

        ch1_ids = len(ch_1_list.keys())
        print(ch1_ids)
        if ch1_ids <= 7:
            interval1 = 255
        else:
            interval1 = int(float(0.512 / ch1_ids) // .0003125)
        print(interval1)
        
        timeout1 = 11.0 / int(poll.Baudrate) + 0.0014
        print(timeout1)  
      
        x1 = serial.Serial(port=poll.channel_1, baudrate=poll.Baudrate, bytesize=poll.bytesize,
                           parity=poll.parity,
                           stopbits=poll.stopbits, timeout=timeout1)
                
        while True:
           
            flag_1 = "pause"

            start = time.time()
            resp = ''
            while resp == '' and time.time() - start < 0.512:
                resp = x1.read(1)
            if resp != '':
                x1.timeout = ((26 * 11.0) / 115200)
                resp += x1.read(26)
                x1.timeout = (11.0 / 115200)

                if len(resp) == 23 and poll.crc_check(resp) == True:

                    if (int(ord(resp[0]))) == 111:
                        relay["CH1_volts"] = str((int(ord(resp[5])) << 8 | int(ord(resp[6]))) * 0.004)
                        if (int(ord(resp[9])) << 8 | int(ord(resp[10]))) > 0X7FFF:
                            current = (((int(ord(resp[9])) << 8 | int(ord(resp[10]))) - 0XFFFF) / 10.0)
                            relay["CH1_current"] = str(current)
                        else:
                            relay["CH1_current"] = str((int(ord(resp[9])) << 8 | int(ord(resp[10]))) / 10.0)

                    print("\t\t\t\t\t\t\t\t\t\t\t\t\t" + " sensor-----2------ " + str(ord(resp[0])) + " " + poll.rec_process(resp))
                    channl_1["CH1" + "_SID_" + str(int(ord(resp[0])))] = poll.rec_process(resp)
            elif len(resp) == 0:
                print("\t\t\t\t\t\t\t\t\t\t\t\t\t" + " -----No Response----- ")
                flag_1 = "stop"
                
            if mysqldb.get_relay_config()[0] != "L":
                stop_thread1 = True
                stop_thread2 = True

            if stop():
                ("Thread killed")
                break
                
            flag_1 = "push"

    @staticmethod
    def Listen_channel_2(stop):

        global stop_thread2
        global stop_thread1
        global flag_2

        ch2_ids = len(ch_2_list.keys())
        print(ch2_ids)
        if ch2_ids <= 7:
            interval2 = 255
        else:
            interval2 = int(float(0.512 / ch2_ids) // .0003125)
        print(interval2)
        
        timeout2 = 11.0 / int(poll.Baudrate) + 0.0014
        print(timeout2)  
      
        x2 = serial.Serial(port=poll.channel_2, baudrate=poll.Baudrate, bytesize=poll.bytesize,
                           parity=poll.parity,
                           stopbits=poll.stopbits, timeout=timeout2)
        
        for l in range(1, 101):
            channl["CH" + str(2) + "_SID_" + str(l)] = "0"

        while True:
            
            flag_2 = "pause"
            start = time.time()
            resp = ''
            while resp == '' and time.time() - start < 0.512:
                resp = x2.read(1)
            if resp != '':
                x2.timeout = ((26 * 11.0) / 115200)
                resp += x2.read(26)
                x2.timeout = (11.0 / 115200)

                if len(resp) == 23 and poll.crc_check(resp) == True:

                    if (int(ord(resp[0]))) == 112:
                        relay["CH2_volts"] = str((int(ord(resp[5])) << 8 | int(ord(resp[6]))) * 0.004)
                        if (int(ord(resp[9])) << 8 | int(ord(resp[10]))) > 0X7FFF:
                            current = (((int(ord(resp[9])) << 8 | int(ord(resp[10]))) - 0XFFFF) / 10.0)
                            relay["CH2_current"] = str(current)
                        else:
                            relay["CH2_current"] = str((int(ord(resp[9])) << 8 | int(ord(resp[10]))) / 10.0)

                    print("\t\t\t\t\t\t\t\t\t\t\t\t\t" + " sensor-----2------ " + str(ord(resp[0])) + " " + poll.rec_process(resp))
                    channl_2["CH2" + "_SID_" + str(int(ord(resp[0])))] = poll.rec_process(resp)

            elif len(resp) == 0:
                print("\t\t\t\t\t\t\t\t\t\t\t\t\t" + " -----No Response----- ")
                flag_2 = "stop"
                
            if mysqldb.get_relay_config()[1] != "L":
                stop_thread1 = True
                stop_thread2 = True
            if stop():
                ("Thread killed")
                break           
            flag_2 = "push"


    @staticmethod
    def merge_two_dicts(x, y):
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    @staticmethod
    def push_data():

        global flag_1
        global flag_2

        relay["CH1_volts"] = "0"
        relay["CH1_current"] = "0"
        relay["CH2_volts"] = "0"
        relay["CH2_current"] = "0"

        for l in range(1, 101):
            channl["CH1_SID_" + str(l)] = "0"
        for j in range(1, 101):
            channl["CH2_SID_" + str(j)] = "0"

        while True:
            #print(flag_1,flag_2)
            mysqldb.toggle_alarm(False) 

            if flag_1 == flag_2 == "push":
			    #Merge the two dictionaries into one for database update
                merge_dict = poll.merge_two_dicts(dict_1,dict_2)
                mysqldb.push_rawdata_to_Database(mysqldb.db_primary,merge_dict)


