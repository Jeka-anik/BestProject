import re
import time,datetime
from datetime import date
from time import ctime
from clickhouse_driver.client import Client
import csv

def reader(filename):
    regexp = r'\w+\s*\d{1,2}\s\d{2}:\d{2}:\d{2}\s\w+\d{2}\s\w+\-\w+\[\d+\]:\s\[(?P<date>\d{4}\-\d{2}\-\d{2}\s\d{2}:\d{2}:\d{2})\]\s\[\w+\s\w+\]\.\w+:\s\{\"\w+\":(?P<userid>\d+)\,\"\w+\":\"(?P<Ip>[\d\.\,\s\.]+)\",\"\w+\":(?P<device>[\w\"\,]+)\"\w+\":\"(?P<httpuseragent>[\w\/\.\s\(\;\-\)\,\:]+)\",\"\w+\":(?P<permission>[\"\w\.]+),\"\w+\":(?P<method>[\"\w]+),\"\w+\":(?P<uri>[\"\/\w\?\=\&]+),\"\w+\":(?P<access>\w+),\"\w+\":(?P<statuscode>[\d]+)(?P<paramsId>[\,\"\}\w\:\{]+)'
    with open(filename) as f:
        log = f.read()

        date_list = re.findall(regexp, log)
    f = open("demo2.txt", "w")
    for line in date_list:
        # print(*date_list, sep='\n')
        pars = re.sub(r'\(\'(?P<date>[\d\-\s\:]+)\'\,\s\'(?P<userid>[\d]+)\'\,\s\'(?P<ip>[\d\.\,\s]+)\'\,\s\'(?P<device>[\"\w\,]+)\'\,\s\'(?P<useragent>[\w\/\.\s\(\)\,\;\:\-]+)\'\,\s\'\"(?P<permission>[\w\.]+)\"\'\,\s\'\"(?P<method>[\w]+)\"\'\,\s\'\"(?P<uri>[\/\w\?\=\&]+)\"\'\,\s\'(?P<access>[\w]+)\'\,\s\'(?P<statuscode>[\d]+)\'\,\s\'[\}|\,](?P<params>[\w\'\"\;\:\{\}\)\,]+)',
        r'\1 ~ \2 ~ \3 ~ \4 ~ \5 ~ \6 ~ \7 ~ \8 ~ \9 ~ \g<10> ~ \g<11>', str(line)+"\n")
        f.write(str(pars))
client = Client('127.0.0.1', password='026943')
print(client.execute('SHOW DATABASES'))
client.execute('Use laravel')
print(client.execute('SHOW TABLES'))

creattable="""CREATE TABLE testbig3
(
    `date` DateTime,
    `userid` String,
    `ip` String,
    `device` String,
    `httpuseragent` String,
    `permission` String,
    `method` String,
    `uri` String,
    `access` String,
    `statuscode` String,
    `paramsid` String
    )
ENGINE = MergeTree
ORDER BY date
"""
client.execute('DROP TABLE IF EXISTS testbig3')
data=[]
start = time.time()

with open(r'demo2.txt') as csvfile :  
	readCSV = csv.reader(csvfile, delimiter='~', quotechar='"', quoting=csv.QUOTE_ALL) # 
	for row in readCSV: 
            x=re.findall(r"\d+\.?\d*",row[0])
            row[0]=datetime.datetime(int(x[0]),int(x[1]),int(x[2]),int(x[3]),int(x[4]),int(x[5]))
            row[1]=str(row[1])
            row[2]=str(row[2])
            row[3]=str(row[3])
            row[4]=str(row[4])
            row[5]=str(row[5])
            row[6]=str(row[6])
            row[7]=str(row[7])
            row[8]=str(row[8])
            row[9]=str(row[9])
            row[10]=str(row[10])
            data.append(row)

 
try:
    
	client.execute(creattable)
	client.execute('INSERT INTO testbig3  VALUES', data,types_check=True)
	end = time.time()
    
	print('clickhouse insertion time', end-start)
 
	
except Exception as e:
    print(e)

if __name__ == '__main__':
    reader('messages-20210512')