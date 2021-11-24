id1 = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

def cmnd_string(id1):
    string = ""
    for i in range(1,max(id1) + 1):
        if i not in id1:
            print("0")
            string+="0"
        else:
            print("1")
            string += "1"
    print(string[::-1])
    string1 = ((hex(int(string[::-1], 2))[2:]).zfill(32))
    print(string1.upper())

cmnd_string(id1)