import json
import threading
import os
#this is for python 3.0 and above. use import thread for python2.0 versions
from threading import*
import time

f_name='data1.json'

def write_file(data, f_name='data1.json'):
    with open(f_name,'w')as f:
        json.dump(data,f,indent=4)

# create_datastore creates a new file data1.json in the local file directory and stores all the key-value data 
# accepts 4 function parameters: key_dict(key name of the dictionary element), value(corresponding key item), timeout(default=0), f_name by default data1.json 
def create_datastore(key_dict,value,timeout=0,f_name='data1.json'):
    
        if os.path.isfile(f_name) and os.path.getsize(f_name)>0:
            with open(f_name) as fp:
                data=json.load(fp)
                data_dict=data
            if key_dict in data_dict:
                print("*****ERROR: ",key_dict," already exists*****") 
            else:
                if(key_dict.isalpha()):
                    if value<=(16*1024*1024) and len(data_dict)<(1024*1020*1024): #file size less than 1GB and JSON object value less than 16KB 
                        if timeout==0:
                            l=[value,timeout]
                        else:
                            l=[value,time.time()+timeout]
                        if len(key_dict)<=32: #key of 32chars
                            data_dict[key_dict]=l
                            print(key_dict, "is created")
                            write_file(data_dict)  
                    else:
                        print("*****ERROR: Memory limit exceeded!! ")
                else:
                    print("*****ERROR: key must contain only alphabets")

        else:
            data_dict={}
            if(key_dict.isalpha()):
                    if value<=(16*1024*1024): #file size less than 1GB and JSON object value less than 16KB 
                        if timeout==0:
                            l=[value,timeout]
                        else:
                            l=[value,time.time()+timeout]
                        if len(key_dict)<=32: 
                            data_dict[key_dict]=l
                            print(key_dict, "is created")
                            write_file(data_dict)


# read_datastore returns the value of the corresponding key in the datastore
# parameters: key to be searched
def read_datastore(key_dict):
     with open(f_name) as fp:
        data=json.load(fp)
        data_dict=data
        print("****** READ OBJECT******")
        if key_dict not in data_dict:
            print("error: given key_dict does not exist in database. Please enter a valid key_dict") 
        else:
            b=data_dict[key_dict]
            if b[1]!=0:
                if time.time()<b[1]: #comparing the present time with expiry time
                    stri=str(key_dict)+":"+str(b[0]) #to return the value in the format of JasonObject i.e.,"key_name:value"

                    print("Value of key ",key_dict," is ",stri)
                else:
                    print("*****ERROR: time-to-live of",key_dict,"has expired") 
            else:
                stri=str(key_dict)+":"+str(b[0])
                print("Value of key ",key_dict," is ",stri)
        fp.close()


#delete_datastore deletes the element from the data file
# parameters: key 
def delete_datastore(key_dict):
     with open(f_name) as fp:
        data=json.load(fp)
        data_dict=data
        print("****** DELETE OBJECT******")
        if key_dict not in data_dict:
            print("*****ERROR: given key_dict does not exist in database. Please enter a valid key_dict") 
        else:
            b=data_dict[key_dict]
            if b[1]!=0:
                if time.time()<b[1]: #comparing the current time with expiry time
                    del data_dict[key_dict]
                    #data_dict.pop(key_dict,None)
                    print(key_dict," is successfully deleted")
                else:
                    print("*****ERROR: time-to-live of",key_dict,"has expired") 
            else:
                #del data_dict[key_dict]
                data_dict.pop(key_dict,None)
                with open('data1.json', 'w') as data_file:
                    data = json.dump(data_dict, data_file,indent=4)
                print(key_dict," is successfully deleted")
                print("\n")
                print("Elements in data store:")
                print(data_dict)
        fp.close()

            
