import serial
import codecs
import time
import sys
import struct
from threading import Thread
# Ser1 = serial.Serial('/dev/ttymxc1',230400,bytesize=8, parity='O', stopbits=1, timeout=.001)
# res = []
# rescnt = 3
# prev = 3
# idx = 0
def crc16(ptrToArray): #//A standard CRC algorithm

    out=0xffff
    carry = 0
    inputSize = len(ptrToArray)
    inputSize+=1
    for l in range(0,inputSize-1):

        out ^= ptrToArray[l]
        for n in range(0,8):
        
            carry = out & 1
            out >>= 1
            if (carry):
                out ^= 0xA001
    return out

def convertSTRtoBytearray(data):
    buff = '\x00'
    for n in range(0, len(data)):
    
            buff[n] = ord(data[n])

    return buff

def ModbusUnicast(DataToWrite):
    Ser1.flush()
    Ser4.flush()

    RxData = ""
    Ser4.write(DataToWrite)
    return RxData

def makeByteBuff(string): #returns tx buffer after adding crc to byte array
    starttime = time.time()
    arr = bytearray(string, 'utf-8')
    for i in range(0,len(arr)):
        if arr[i] <= 57:
            arr[i] = arr[i]-48
        elif arr[i] >= 65 and  arr[i] <= 70:
            arr[i] = arr[i]-55
    pos = 0
    for i in range(0,len(arr)):
        arr[i] = arr[pos]<<4|arr[pos+1]
        if pos+2 == len(arr):
            pos = i+1
            break
        pos += 2
    #if pos == 2:
        #pos = 4
    crc = crc16(arr[:pos])
    arr[pos] = crc%256
    arr[pos+1] = crc/256
    print(len(arr[:pos+2]))
    #print("Total time consumed " + str(time.time()- starttime))
    return arr[:pos+2]

def getFloat(buff):
    f_data = struct.unpack('!f',buff)

    return f_data

def printdata(qtyofresp,qtyofrsidpresent):
    for i in range(0,(qtyofresp)*(qtyofrsidpresent)):
        global res
        global rescnt
        global prev
        global idx
        start = time.time()
        #print("resp --> " + codecs.encode(res[idx],"HEX"))
        while res[i] == "00000000000000000000":
            #print(time.time() - start)
            if time.time() - start >= 0.1:
                break;
        #print("resp --> " + res + " length is " + str(len(res[i])))
        #print(i)
        new_Res = res[i]
        #print(codecs.encode(res[i],"HEX"))
        #print(codecs.encode(new_Res,"HEX"))
        #res = ""
        Ser4.flush()
        datatoPrint = ""
        if new_Res != "":
            if len(new_Res) == 23:

                resBytebuff = new_Res
                datatoPrint = "SID --> " + str(ord(resBytebuff[0]))
                datatoPrint += " Errbyte "
                datatoPrint += codecs.encode(resBytebuff[3:5],"HEX")

                datatoPrint += " Volt "
                datatoPrint += str(round((float)(int(ord(resBytebuff[5]))<<8|int(ord(resBytebuff[6])))*0.01539,1))
                datatoPrint += " temp "
                datatoPrint += str(round((float)((ord(resBytebuff[7]))<<8|int(ord(resBytebuff[8])))*0.00107421*100,1))
                datatoPrint += " Status "
                datatoPrint += codecs.encode(resBytebuff[9:11],"HEX")
                #datatoPrint += " PStatus "
                #datatoPrint += codecs.encode(resBytebuff[13:15],"HEX")

                datatoPrint += "\t"
                datatoPrint += " ZAvg "
                datatoPrint += str(int(ord(resBytebuff[11]))<<8|int(ord(resBytebuff[12])))
                #datatoPrint += "\t"
                datatoPrint += " ZStdd "
                datatoPrint += str(getFloat(resBytebuff[13:17]))[1:6]
                #datatoPrint += "\t"
                datatoPrint += " ZRstdd "
                Zrange = int(ord(resBytebuff[17]))<<8|int(ord(resBytebuff[18]))
                try:
                    datatoPrint += str(round((Zrange/getFloat(resBytebuff[13:17])[0]),2))
                except:
                    datatoPrint += "0"
                #datatoPrint += "\t"
                datatoPrint += " ZMax Rate "
                datatoPrint += str(int(ord(resBytebuff[19]))<<8|int(ord(resBytebuff[20])))
                '''
                datatoPrint += "\t"
                datatoPrint += " YAvg "
                datatoPrint += str(int(ord(resBytebuff[21]))<<8|int(ord(resBytebuff[22])))
                #datatoPrint += "\t"
                datatoPrint += " YStdd "
                datatoPrint += str(getFloat(resBytebuff[23:27]))[1:6]
                #datatoPrint += "\t"
                datatoPrint += " YRstdd "
                Yrange = int(ord(resBytebuff[27]))<<8|int(ord(resBytebuff[28]))
                try:
                    datatoPrint += str(round((Yrange/getFloat(resBytebuff[23:27])[0]),2))
                except:
                    datatoPrint += "0"
                #datatoPrint += "\t"
                datatoPrint += " YMax Rate "
                datatoPrint += str(int(ord(resBytebuff[29]))<<8|int(ord(resBytebuff[30])))
                
                datatoPrint += "\t"
                datatoPrint += " XAvg "
                datatoPrint += str(int(ord(resBytebuff[31]))<<8|int(ord(resBytebuff[32])))
                #datatoPrint += "\t"
                datatoPrint += " XStdd "
                datatoPrint += str(getFloat(resBytebuff[33:37]))[1:6]
                #datatoPrint += "\t"
                datatoPrint += " XRstdd "
                Xrange = int(ord(resBytebuff[37]))<<8|int(ord(resBytebuff[38]))
                try:
                    datatoPrint += str(round((Xrange/getFloat(resBytebuff[33:37])[0]),2))
                except:
                    datatoPrint += "0"
                #datatoPrint += "\t"
                datatoPrint += " XMax Rate "
                datatoPrint += str(int(ord(resBytebuff[37]))<<8|int(ord(resBytebuff[38])))
                '''
                #rescnt += 1

                #print(resBytebuff[0] - prev)
                #prev =  resBytebuff[0]
                #print(idx)
        #	else:
                #datatoPrint = "Received Unexpected no. of bytes, received Data Is -> " + new_Res
            #print("SID --> "),
            print(datatoPrint)
        else:
            print(res[idx])
            print("Noresponse index " + str(idx+14)  )
def readSerial(qtyofresp,qtyofrsidpresent):
    global res
    global idx
    data = ""
    for i in range(0,(qtyofresp)*(qtyofrsidpresent)):
        res[i]= Ser4.read(23)
        #print("ser" + str(i))
        #print(len(res[i]))
        #print(codecs.encode(res[i],"HEX"))
        idx = i
'''
enum TimesharingFrame
 {
     SID_idx,
     fc_idx,
     regstartAddr_high_idx,
     regstartAddr_low_idx,
     regQty_idx,
     interval_idx,
     startSID_idx,
     endSID_idx,
     qtyofresp_high_idx,
     qtyofresp_low_idx,
     nosofSIDsharingTime_idx
 }TimesharingframIDX;
'''

regstartAddr = 0x8500
regQty = 9
interval = 16
startSID = 1
endSID = 127
qtyofrsidpresent = 79 # 20#14
res = [bytearray("00000000000000000000", 'utf-8')]*qtyofrsidpresent
#
qtyofresp = 1
Ser4 = serial.Serial('/dev/ttymxc4',115200,bytesize=8, stopbits=2, timeout= .1)

'''
#read firmware version and fuses
for i in range(1,96):
    ModbusUnicast(makeByteBuff(format(i,'02X')+"03FF000008"))
    print(codecs.encode(Ser4.read(21),"HEX"))
'''
id1 = range(startSID,endSID+1)

def cmnd_string(id1):
    string = ""
    for i in range(1,max(id1) + 1):
        if i not in id1:
            #print("0")
            string+="0"
        else:
            #print("1")
            string += "1"
    print(string[::-1])
    string1 = ((hex(int(string[::-1], 2))[2:]).zfill(32))
    print(string1.upper())
    return string1.upper()

cmnd_string(id1)

commandString = format(0,'02X')+format(0x42,'02X')+format(regstartAddr,'04X')+format(regQty ,'02X')+format(interval,'02X')+format(startSID ,'02X')+format(endSID ,'02X')+format(qtyofrsidpresent ,'02X')
commandString +=  "000000007FE7FFFFFFFFFBFFFFFFE000"#"000000007FE7FFFFFFFFFBFFFFFFE000" #"0000000000000000000000000FFFE000"#"00000000000000000003FFFFFFFFEFA8"#"00000000FFEFFFFFFFFFFFFFFFFFEFA8" #"00000000FFFFFFFFFFFFFFFFFFFFFFFF"#"00000000FFFEFFFFFFFFFFFFFFFF0000"
datatosend = makeByteBuff(commandString)
print("Command Frame---> " + codecs.encode(datatosend,"HEX" ))


t1 = Thread(target = readSerial, args = (qtyofresp,qtyofrsidpresent))
t2 = Thread(target = printdata, args = (qtyofresp,qtyofrsidpresent))


t1 .setDaemon(True)
t2 .setDaemon(True)

ModbusUnicast(datatosend)


t1 .start()
t2 .start()
starttime = time.time()
t1 .join()
t2 .join()

endtime = time.time()
print("Total time consumed " + str(endtime - starttime))
expectedtime = (float)(qtyofrsidpresent)*(float)(interval)*(float)(qtyofresp)*0.0003125
print("expected time " + str(expectedtime))
print("diff is  " + str((endtime - starttime) - expectedtime))
print("total response " + str(rescnt-3))
Ser4.close()
Ser1.close()