# Python program killing
# threads using stop
# flag

from classes import relay_board
from classes import mysqldb
from classes import poll
from classes import ch_1_list
from classes import ch_2_list
from classes import channl_1
from classes import channl_2
from threading import Thread
import threading
import time
#from Queue import Queue
import classes

stop_thread1 = False
stop_thread2 = False


def Querry_1(stop):

    global stop_thread1
    global stop_thread2

    while True:
        if mysqldb.get_relay_config()[0] != "Q":
              stop_thread1 = True
              stop_thread2 = True

        if stop():
            break

        time.sleep(1)
        print('Querry channel 1')

def Querry_2(stop):

    global stop_thread2
    global stop_thread1

    while True:
        if mysqldb.get_relay_config()[1] != "Q":
              stop_thread1 = True
              stop_thread2 = True

        if stop():
            break

        time.sleep(1)
        print('Querry channel 2')

def Listen_1(stop):

    global stop_thread2
    global stop_thread1

    while True:
        if mysqldb.get_relay_config()[0] != "L":
            stop_thread1 = True
            stop_thread2 = True

        if stop():
            break

        time.sleep(1)
        print('listener channel 1')

def Listen_2(stop):

    global stop_thread2
    global stop_thread1

    while True:
        if mysqldb.get_relay_config()[1] != "L":
            stop_thread1 = True
            stop_thread2 = True

        if stop():
            break

        time.sleep(1)
        print('listener channel 2')