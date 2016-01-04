# -*- coding: utf-8 -*-
import psycopg2
from datetime import datetime
import re
f = open('/home/postgres/tmp.txt','r')

def IntInString(s):
        try:
                return int(re.search(r'\d+', s).group())
        except AttributeError:
                return 1

def FrequencySeconds(s):
        if "分鐘" in s : return 60*IntInString(s)
        if "小時" in s : return 60*60*IntInString(s)
        return 86400

while True:
        line = f.readline()
        if not line:break
        tmp = line.split("\t")
        FS = FrequencySeconds(tmp[6])
        LastUpdate = datetime.strptime(tmp[7].split("+")[0],'%Y-%m-%d %H:%M:%S.%f')
        NowTime = datetime.strptime(datetime.now().isoformat(), '%Y-%m-%dT%H:%M:%S.%f')
        DeltaTime = (NowTime-LastUpdate).total_seconds()
        #print (NowTime-LastUpdate).total_seconds()
        if FS < DeltaTime:
                sql_query = "update table_list set 更新時間=now() where tid='"+tmp[0]+"';"
                conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
                cur = conn.cursor()
                cur.execute(sql_query)
                conn.commit()
                conn.close()
                print tmp[6]+"\t"+str(FS)
