import re
import sys

import time,datetime
from datetime import date
from time import ctime
from clickhouse_driver.client import Client
import csv
import types

def reader(filename):
    regexpinclud = r'(?P<Date>\[\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\])\s\w+.\w+:\s(?P<text>[\w\s\(\)\/\.]+)(?P<text2>\{[\w\"\:\[\]\,\s\(\)\/\)]+)'
    regexpsql = r'(?P<Date>\[\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\])\s\w+.\w+:\s(?P<error>[\w\[\]\s]+):\s\w+\s\w+:\s(?P<numbererror>[\d]+)\s\w+:(?P<ERROR>[\s\w\"\_\:\.\$\^\(\*\)\=\'\{\}\,\[\]]+)'
    # regexpsql = r'(?P<Date>\[\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\])\s\w+.\w+:\s(?P<text>[\w\s\(\)\/\.]+)(?P<text2>[\{\w\"\:\[\]\,\s\(\)]+)'
    regexp = r'\[(?P<Date>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\]\s\w+.\w+:\s\[[E]\w+\]\s\w+\s\w+:\s(?P<Code>\d{3})\s\|\s\w+:\s(?P<Message>[\w\s]+.*)\|\s(?P<Errors>[\w\s]+):\s+\|\s\w+:\s(?P<code>\d+)'
    with open(filename) as f:
        log = f.read()

        date_list = re.findall(regexp, log)
        date_list2 = re.findall(regexpsql, log)
        date_list3 = re.findall(regexpinclud, log)
    # print(*date_list, sep='\n') 
    myfile = open("demo.txt", "w")
    # myfile.write(str(date_list) + "," + "\n")
    # f.write(str(*date_list, sep='\n'))
    for line in date_list:
        # print(re.sub(r'(?P<Date>\'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\'),\s\'(\d{3})\'\,\s([\w\s\'\.]+)[,|.]\s([\w\s\']+)\,\s\'(\d+)\'',
        #             r'\1, \2, \3, \4, \5', str(line) + "," + "\n"))
        varrr = re.sub(r'(?P<Date>\'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\'),\s\'(\d{3})\'\,\s([\w\s\'\.]+)[,|.]\s([\w\s\']+)\,\s\'(\d+)\'',
                     r'\1, \2, \3, \4, \5  ', str(line) + "\n")
        # for line11 in line:
        #     myfile.write("datetime.fromisoformat" + str(line11))
        # print(*date_list, sep='\n')
        myfile.write(varrr)
    # for line2 in date_list2:
        # print(*date_list2, sep='\n')
        # myfile.write(str(line2) + "\n")
    # for line3 in date_list3:
        # print(*date_list3, sep='\n')
        # myfile.write(str(line3) + "\n")

client = Client('127.0.0.1', password='026943')
print(client.execute('SHOW DATABASES'))
client.execute('Use laravel')
print(client.execute('SHOW TABLES'))
    # client.execute('SHOW DATABASES')
    # [[datetime.fromisoformat('2021-05-27 11:00:17'), 401, 'Authentication fail', 'Errors', 404 ], [datetime.fromisoformat('2021-05-27 13:00:17'), 404, 'Authentication fail', 'Errors', 502 ], [datetime.fromisoformat('2021-05-27 14:00:17'), 404, 'Authentication fail', 'Errors', 505 ]]
    # client.execute('INSERT INTO laravel12 (date, code, text, errors, code2) VALUES ',
    # [])

creattable="""CREATE TABLE test1
(
    `date` DateTime,
    `code` Int,
    `text` String,
    `errors` String,
    `code2` Int
)
ENGINE = MergeTree
ORDER BY date
"""
client.execute('DROP TABLE IF EXISTS test1')
# client.execute(creattable)
data=[]
start = time.time()

with open(r'demo.txt') as csvfile:  
	readCSV = csv.reader(csvfile, delimiter=',')  
	for row in readCSV: 
            x=re.findall(r"\d+\.?\d*",row[0])
            row[0]=datetime.datetime(int(x[0]),int(x[1]),int(x[2]),int(x[3]),int(x[4]),int(x[5]))
            row[1]=int(row[1])
            row[2]=str(row[2])
            row[3]=str(row[3])
            x=re.findall(r'\d+',row[4])
            row[4]=int(int(x[0]))
            data.append(row)
 
try:
	client.execute(creattable)
	client.execute('INSERT INTO test1  VALUES', data,types_check=True)
	end = time.time()
	print('clickhouse insertion time', end-start)
 
	
except Exception as e:
    print(e)
if __name__ == '__main__':
    reader('laravel-2021-05-27.log')