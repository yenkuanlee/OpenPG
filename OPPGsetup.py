# -*- coding: utf-8 -*-
import os,sys
import psycopg2
def MountFile(local_ip,remote_ip):
	command = ""
        command += "sudo mkdir /home/postgres;"
        command += "sudo chown -R postgres:postgres /home/postgres ;"
	os.system(command)
        os.system("ssh "+remote_ip+" -t \""+command+"\"")
	os.system("sudo -u postgres ssh "+remote_ip+" \"sshfs "+local_ip+":/home/postgres /home/postgres -o nonempty\"")
	os.system("sudo -u postgres touch /home/postgres/tmp.txt")

def DBsetup():
	#os.system("sudo sed -i \"s/psycopg2.connect.*/psycopg2.connect(database=\\\"openpg\\\", user=\\\"guest\\\", password=\\\"guest\\\", host=\\\"127.0.0.1\\\", port=\\\"5432\\\")/g\"")
	server_list = ['fdw_write','web_csv','Web_XML','web_json']
	os.system("sudo -u postgres createdb openpg -O guest")
	os.system("sudo -u postgres psql -c \"CREATE EXTENSION multicorn;\" openpg")
	for server in server_list:
		os.system("sudo -u postgres psql -c \"drop server "+server+" CASCADE;\" openpg")
	os.system("sudo -u postgres psql -c \"CREATE server fdw_write foreign data wrapper multicorn options (wrapper 'multicorn.FDWwrite.FDWwrite'); \" openpg")
	os.system("sudo -u postgres psql -c \"CREATE SERVER web_csv foreign data wrapper multicorn options (wrapper 'multicorn.WebCsvFdw.WebCsvFdw');\" openpg")
	os.system("sudo -u postgres psql -c \"CREATE SERVER Web_XML foreign data wrapper multicorn options (wrapper 'multicorn.WebXML.XMLFdw');\" openpg")
	os.system("sudo -u postgres psql -c \"CREATE SERVER web_json foreign data wrapper multicorn options (wrapper 'multicorn.WebJson.WebJson');\" openpg")
	for server in server_list:
		os.system("sudo -u postgres psql -c \"ALTER SERVER "+server+" OWNER TO guest;\" openpg")
	os.system("sudo -u postgres psql -c \"create schema odtp;\" openpg")
	os.system("sudo -u postgres psql -c \"create schema ft_odtp;\" openpg")
	os.system("sudo -u postgres psql -c \"ALTER SCHEMA odtp OWNER TO guest;\" openpg")
	os.system("sudo -u postgres psql -c \"ALTER SCHEMA ft_odtp OWNER TO guest;\" openpg")

def TableSetup():
	create_table_list = "CREATE foreign table Table_List (tid text,表格名稱 text,原始資料格式 text,欄位 text,網址 text,描述 text,更新頻率 text,更新時間 text,CSV_skip_header text,CSV_delimiter text,elem_tag text) server fdw_write options (test_type 'returning',tx_hook 'true',filename '/home/postgres/tmp.txt');"
	create_open_table = "CREATE MATERIALIZED VIEW opentable AS select * from table_list;"
	conn = psycopg2.connect(database="openpg", user="guest", password="guest", host="127.0.0.1", port="5432")
        cur = conn.cursor()
        cur.execute(create_table_list)
	cur.execute(create_open_table)
        conn.commit()
        conn.close()


#MountFile(sys.argv[1],sys.argv[2])
DBsetup()
TableSetup()
