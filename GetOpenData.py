# -*- coding: utf-8 -*-
import pycurl
from StringIO import StringIO
import chardet
import csv
import psycopg2
import sys
import urllib2
import xml.etree.ElementTree as ET
import json
from collections import OrderedDict
import unicodedata
def GetCSVSchema(url):
	f = urllib2.urlopen(url)
	tmp = f.readline()#.replace("\r","\n")
	schema = tmp.split("\n")[0]
	if "," not in schema : return "BAD"
	if ",," in schema : return "BAD"
	if schema[len(schema)-1]==',':return "BAD"
	code =  chardet.detect(schema)['encoding'].lower()
	if code != 'utf-8':
		schema = schema.decode('Big5').encode('utf-8')
		schema = schema[:len(schema)-1].split("\r")[0]
	Sset = set()
	Slist = schema.split(",")
	for x in Slist:
		if x in Sset : return "BAD"
		Sset.add(x)
	return schema
	
def GetJsonSchema(url):
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue()
	j = json.loads(content, object_pairs_hook=OrderedDict)
	X = ""
	try:
		for x in j[0].keys():
			X = X+x+" text,"
	except KeyError:
			return "BAD"
	return X[:len(X)-1].encode('utf-8','ignore')

def XmlTagProcessing(root,TagCount,ChildTagSet):
	if len(root)>0:
		RootTag = root.tag
		if RootTag in TagCount:
			TagCount[RootTag] += 1
		else : TagCount[RootTag] = 1
		if RootTag not in ChildTagSet:ChildTagSet[RootTag] = set()
		for child in root:
			ChildTagSet[RootTag].add(child.tag)
			XmlTagProcessing(child,TagCount,ChildTagSet)

def FindElemTag(url):
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue()
	root = ET.fromstring(content)
	Dic=dict()
	ChildTag = dict()
	XmlTagProcessing(root,Dic,ChildTag)
	max = 0	
	key = ""
	for x in Dic:
		if Dic[x] > max :
			max = Dic[x]
			key = x
	if key=="":
		return "BAD",set()
	return key,ChildTag[key]
	
def GetDownloadUrl(OID):
	url = "http://data.taipei/opendata/datalist/apiAccess?scope=datasetMetadataSearch&q=id:"+OID
	storage = StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, storage.write)
        c.perform()
        c.close()
        content = storage.getvalue()
	if len(content.split("\"resourceId\":\""))<2:
		print OID
		return "error"
	RID = content.split("\"resourceId\":\"")[1].split("\"")[0]
	Durl = "http://data.taipei/opendata/datalist/datasetMeta/download?id="+OID+"&rid="+RID
	return Durl

def GetDownloadUrlWithManyData(OID):
	D = dict()
	url = "http://data.taipei/opendata/datalist/apiAccess?scope=datasetMetadataSearch&q=id:"+OID
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue()
	j = json.loads(content)
	AllData = j['result']['results'][0]['resources']
	for x in AllData:
		try:
			TableName = x['resourceName'].encode('utf-8','ignore')
			Url = "http://data.taipei/opendata/datalist/datasetMeta/download?id="+OID+"&rid="+x['resourceId'].encode('utf-8','ignore')
			D[TableName]=Url
		except KeyError:
			return set()
	return D
	

def RepresentsInt(s):
	try: 
		int(s)
		return True
	except ValueError:
		return False
		
def ShitHead(s):
	Stmp = s.split(",")
	if len(Stmp)==1:
		if RepresentsInt(s[0]):s="_"+s
		return s
	for i in range(len(Stmp)):
		x = Stmp[i]
		if x=="":return "BAD"
		if RepresentsInt(x[0]) :
			if i == 0 : s = s.replace(x+",","_"+x+",")
			else : s = s.replace(","+x,",_"+x)
	return s

def good_string(s):
	s=s.replace("(","__")
	s=s.replace(")","__")
	s=s.replace("-","_")
	s=s.replace("/","_")
	s=s.replace("?","_")
	s=s.replace(" ","")
	s=s.replace(".","_")
	s=s.replace("[","_")
	s=s.replace("]","_")
	s=s.replace("%","persent")
	s=s.replace("'","")
	return s
		
url = "http://data.taipei/opendata/rule/downloadList?output=csv"
storage = StringIO()
c = pycurl.Curl()
c.setopt(c.URL, url)
c.setopt(c.WRITEFUNCTION, storage.write)
c.perform()
c.close()
content = storage.getvalue()
print "get report already"

L =  content.split("\n")

ImportantSet = {8,6}
ImportantCode = {"utf-8,"}
ImportantFormat = {"csv,","xml,","json,","many_csv","many_xml","many_json"} 
#ImportantFormat = {"json,"}
reader = csv.reader(L[:len(L)-1], delimiter=',')
TableSet = set()

IDset = set()
#f = open('/home/postgres/tmp.txt','r')
#while True:
#        line = f.readline()
#        if not line:break
#        IDset.add(int(line.split("\t")[0][1:]))
Tnumber = 0
if len(IDset)!=0:
	Tnumber = max(IDset)+1
TID = ""
for line in reader:
	update_frequency = line[11].decode('big5','ignore').encode('utf8')
	url = line[12]
	encode = line[13].lower()
	format = line[17].lower()
	
	Fset = set()
	Ftmp = format.split(",")
	Eset = set()
	Etmp = encode.split(",")
	if len(Ftmp) > 2 and len(Etmp) > 2: # data more than one
		for x in Ftmp:
			if not x:continue
			Fset.add(x)
		for x in Etmp:
			if not x:continue
			Eset.add(x)
		if len(Fset)==1 and len(Eset)==1:#only one type data
			format = "many_"+list(Fset)[0]
			encode = list(Eset)[0]+","
			
	if "oid=" not in url or encode not in ImportantCode or format not in ImportantFormat : continue
	
	for i in range(len(line)):
		if i not in ImportantSet : continue
		if str(chardet.detect(line[i])['encoding']).lower()!='utf-8':
			line[i]=line[i].decode('Big5').encode('utf-8')
	
	#table_name = ShitHead(good_string(line[8]))
	table_name = line[8]
	if table_name in TableSet : continue
	else : TableSet.add(table_name)
	
	discription =good_string(line[6])
	#fuckyou_schema = line[21]
	
	
	debug = 0
	
	schema = "NotCSV"
	if format=="csv,":
		url = GetDownloadUrl(url.split("oid=")[1])
		schema = ShitHead(good_string(GetCSVSchema(url)))
		if schema == "BAD" : continue
		if schema[len(schema)-1]=="\r":continue
		schema = schema.replace(","," text,")+" text"
		if ", text" in schema:schema=schema.replace(", text","")
		if str(chardet.detect(schema)['encoding'])=="None":continue
		
		print table_name+"\t"+format
		TID = "odtp.t"+str(Tnumber)
		Tnumber += 1
		sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','csv','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','');"
		conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
		cur = conn.cursor()
		cur.execute(sql_query)
		conn.commit()
		conn.close()
		
	elif format=="xml,":
		print table_name+"\t"+format
		url = GetDownloadUrl(url.split("oid=")[1])
		ElemTag,ColumnSet = FindElemTag(url)
		if ElemTag=="BAD":continue
		if ":" in ElemTag : continue
		schema = ""
		Check = set()
		for x in ColumnSet :
			if x.lower() in Check:continue
			else:Check.add(x.lower())
			schema += x+" text,"
		schema = schema[:len(schema)-1]
		TID = "odtp.t"+str(Tnumber)
                Tnumber += 1
		sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','xml','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','"+ElemTag+"');"
		conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
		cur = conn.cursor()
		cur.execute(sql_query)
		conn.commit()
		conn.close()
	
	elif format=="json,":
		print table_name+"\t"+format
		url = GetDownloadUrl(url.split("oid=")[1])
		schema = GetJsonSchema(url)
		TID = "odtp.t"+str(Tnumber)
                Tnumber += 1
		sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','json','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','');"
		conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
		cur = conn.cursor()
		cur.execute(sql_query)
		conn.commit()
		conn.close()
	elif format=="many_csv":
		FileDic = GetDownloadUrlWithManyData(url.split("oid=")[1])
		for file in FileDic:
			table_name = ShitHead(good_string(file))
			url = FileDic[file]
			schema = ShitHead(good_string(GetCSVSchema(url)))
			if schema == "BAD" : continue
			if schema[len(schema)-1]=="\r":continue
			schema = schema.replace(","," text,")+" text"
			if ", text" in schema:schema=schema.replace(", text","")
			if str(chardet.detect(schema)['encoding'])=="None":continue
			
			print table_name+"\t"+str(len(table_name))
			TID = "odtp.t"+str(Tnumber)
	                Tnumber += 1
			sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','csv','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','');"
			conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
			cur = conn.cursor()
			cur.execute(sql_query)
			conn.commit()
			conn.close()

	elif format=="many_xml":
		FileDic = GetDownloadUrlWithManyData(url.split("oid=")[1])
		for file in FileDic:
			table_name = ShitHead(good_string(file))
			url = FileDic[file]
			ElemTag,ColumnSet = FindElemTag(url)
			if ElemTag=="BAD":continue
			if ":" in ElemTag : continue
			schema = ""
			Check = set()
			for x in ColumnSet :
				if x.lower() in Check:continue
				else:Check.add(x.lower())
				schema += x+" text,"
			schema = schema[:len(schema)-1]
			
			print table_name+"\t"+str(len(table_name))
			TID = "odtp.t"+str(Tnumber)
	                Tnumber += 1
			sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','xml','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','"+ElemTag+"');"
			conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
			cur = conn.cursor()
			cur.execute(sql_query)
			conn.commit()
			conn.close()
			
	elif format=="many_json":
		FileDic = GetDownloadUrlWithManyData(url.split("oid=")[1])
		for file in FileDic:
			table_name = ShitHead(good_string(file))
			url = FileDic[file]
			schema = GetJsonSchema(url)
			if schema=="BAD":continue
			TID = "odtp.t"+str(Tnumber)
	                Tnumber += 1
			sql_query = "insert into Table_List(tid,表格名稱,原始資料格式,欄位,網址,描述,更新頻率,更新時間,csv_skip_header,csv_delimiter,elem_tag) VALUES ('"+TID+"','"+table_name+"','json','"+schema+"','"+url+"','"+discription+"','"+update_frequency+"',now(),'1',',','');"
			conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
			cur = conn.cursor()
			cur.execute(sql_query)
			conn.commit()
			conn.close()
