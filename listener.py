import serial
import codecs
from classes import poll
import time

timeout2 = 11.0/115200
print(timeout2)

ch2 = serial.Serial('/dev/ttymxc1', 115200,bytesize = 8,parity = 'N',stopbits = 2,timeout = timeout2)
resList = ['']*110

ch = ''
interval = 0
'''
while ch == '': 
	ch = ch2.read(1)
	#print(ch)

	
	if ch != '':
		if ch[0] == '\x00':
			cnt  = 0
			while cnt < 2:
				ch += ch2.read(1)
				cnt += 1

			if ch[1] == '\x42':

				for i in range(0, 25):
					ch +=  ch2.read(1)
			print(len(ch))
			if len(ch) < 27:
				ch = ''
			else:
				print("data : ",codecs.encode(ch,"HEX"))
				if(poll.crc_check(ch) == True):
					interval = ((float)(int(ord(ch[5])))*(0.0003125)/(11.00/int(poll.Baudrate)))
					print(int(interval))
				else:
					ch = ''
		else:
			ch = ''	
'''		
while True:
	ch = ''
	count = 0
	silentcnt = time.time()
	while ch == '': 
		ch = ch2.read(1)
		#silentcnt += 1
	print(time.time() - silentcnt)

	silentcnt = time.time()

	ch += ch2.read(27)
			
	print("count : ", count, " data : ", codecs.encode(ch,"HEX"))
