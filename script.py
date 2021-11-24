from classes import relay_board
from classes import mysqldb
from classes import poll
from classes import ch_1_list
from classes import ch_2_list
from threading import Thread
import threading
import time
import classes

def Redundant():

    while True:

        while mysqldb.get_relay_config() == ('Q','Q'): 
    
            classes.stop_thread1 = False
            classes.stop_thread2 = False
                
            print("querry on both channels")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            poll.readUID(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            poll.read_fuse_byte(ch_1_list,poll.channel_1,mysqldb.db_primary,1)

            poll.readUID(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            poll.read_fuse_byte(ch_2_list,poll.channel_2,mysqldb.db_primary,2)

            th1 = Thread(target = poll.Querry_channel_1, args=(lambda: classes.stop_thread1,))
            th2 = Thread(target = poll.Querry_channel_2, args=(lambda: classes.stop_thread2,))
   
            th1.start()
            th2.start()
            th1.join()
            th2.join()
        

        while mysqldb.get_relay_config() == ('Q','L'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("querry on channel 1 , Listen on Channel 2")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")           
            time.sleep(relay_board.power_up)

            poll.readUID(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            poll.read_fuse_byte(ch_1_list,poll.channel_1,mysqldb.db_primary,1)

            th1 = Thread(target = poll.Querry_channel_1, args=(lambda: classes.stop_thread1,))
            th2 = Thread(target = poll.Listen_channel_2, args=(lambda: classes.stop_thread2,))
   
            th1.start()
            th2.start()
            th1.join()
            th2.join()

        while mysqldb.get_relay_config() == ('L','Q'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Listen on channel 1 , Querry on Channel 2")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            poll.readUID(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            poll.read_fuse_byte(ch_2_list,poll.channel_2,mysqldb.db_primary,2)

            th1 = Thread(target = poll.Listen_channel_1, args=(lambda: classes.stop_thread1,))
            th2 = Thread(target = poll.Querry_channel_2, args=(lambda: classes.stop_thread2,))
   
            th1.start()
            th2.start()
            th1.join()
            th2.join()


        while mysqldb.get_relay_config() == ('L','L'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Listen on both Channels")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            th1 = Thread(target = poll.Listen_channel_1, args=(lambda: classes.stop_thread1,))
            th2 = Thread(target = poll.Listen_channel_2, args=(lambda: classes.stop_thread2,))
   
            th1.start()
            th2.start()
            th1.join()
            th2.join()


        while mysqldb.get_relay_config() == ('Q','X'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Only Querry on Channel 1")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            poll.readUID(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            poll.read_fuse_byte(ch_1_list,poll.channel_1,mysqldb.db_primary,1)

            th1 = Thread(target = poll.Querry_channel_1, args=(lambda: classes.stop_thread1,))
            th1.start()
            th1.join()


        while mysqldb.get_relay_config() == ('L','X'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Only listen on Channel 1")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            th1 = Thread(target = poll.Listen_channel_1, args=(lambda: classes.stop_thread1,))
            th1.start()
            th1.join()

        while mysqldb.get_relay_config() == ('X','Q'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Only Querry on Channel 2")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")   
            time.sleep(relay_board.power_up)

            poll.readUID(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            poll.read_fuse_byte(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
           
            th1 = Thread(target = poll.Querry_channel_2, args=(lambda: classes.stop_thread2,))
            th1.start()
            th1.join()

        while mysqldb.get_relay_config() == ('X','L'):

            classes.stop_thread1 = False
            classes.stop_thread2 = False

            print("Only listen on Channel 2")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            th1 = Thread(target = poll.Listen_channel_2, args=(lambda: classes.stop_thread2,))
            th1.start()
            th1.join()

        while mysqldb.get_relay_config() == ('X','X'):
            print("maintainance mode") 


def NonRedundant():

    while True:

        if mysqldb.get_relay_config() == ('Q','Q'): 
    
            classes.stop_thread1 = False
            classes.stop_thread2 = False
                
            print("querry on both channels")

            print("Waiting for " + str(relay_board.power_up) + " seconds to power up...")      
            time.sleep(relay_board.power_up)

            poll.readUID(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            poll.read_fuse_byte(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            poll.read_setpoint(ch_1_list,poll.channel_1,mysqldb.db_primary,1)
            mysqldb.fetch_Setpoint(mysqldb.db_primary,1,poll.channel_1)

            poll.readUID(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            poll.read_fuse_byte(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            poll.read_setpoint(ch_2_list,poll.channel_2,mysqldb.db_primary,2)
            mysqldb.fetch_Setpoint(mysqldb.db_primary,2,poll.channel_2)


            th1 = Thread(target = poll.Querry_channel_1, args=(lambda: classes.stop_thread1,))
            th2 = Thread(target = poll.Querry_channel_2, args=(lambda: classes.stop_thread2,))
   
            th1.start()
            th2.start()
            th1.join()
            th2.join()


mysqldb.connect_db(mysqldb.db_primary)

if mysqldb.fetch_System_Mode(mysqldb.db_primary) == "NonRedundant":
    mysqldb.fetch_both_channel_sensor_config(mysqldb.db_primary)
    main = NonRedundant

elif mysqldb.fetch_System_Mode(mysqldb.db_primary) == "Redundant":
    mysqldb.fetch_sensor_config(mysqldb.db_primary)
    main = Redundant

poll.read_sensor_config()
data_table_exists = mysqldb.verify_raw_Database(mysqldb.db_primary)
if not data_table_exists:
    mysqldb.create_raw_Database(mysqldb.db_primary)

mainThread1= Thread(target = mysqldb.fetch_relay_config, args =[mysqldb.db_primary])
mainThread2= Thread(target = main)
mainThread3= Thread(target = poll.push_data)

mainThread1.start()
mainThread2.start()
mainThread3.start()

mainThread1.join()
mainThread2.join()
mainThread3.join()