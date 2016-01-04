先在master和slave做(0)設定

(1)~(4) 可執行 $ python run_script.py local_ip remote_ip

(5) 依照個人需求決定是否到slave做設定


.....................................................................................


(0) master/slave的PG設定，讓彼此之間的postgres帳號可以互連
	$ sudo apt-get update

	$ sudo apt-get install postgresql postgresql-contrib postgresql-client

	$ sudo passwd postgres

		enter the password of postgres

	$ sudo -u postgres ssh-keygen

	$ ssh-copy-id $IP_address_of_opposite_server




(1) master環境設定 (multicorn, git, make, pycurl, 程式移到multicorn目錄)

	$ sudo python InstallOnMaster.py


	
(2) 多機PG設定(master/slave db同步，master可讀寫，slave只可讀)

	$ sudo -u postgres python PGdeploy.py local_ip remote_ip




(3) 建立OpenPG框架

	$ python OPPGsetup.py local_ip remote_ip



(4) load open data from 台北市開放資料

	$ python GetOpenData.py

	

(5) open slave (on slave)
	$ sudo service postgresql stop
	
	$ vi /etc/postgresql/9.3/main/pg_hba.conf

		host    all             all             all            md5

	$ sudo service postgresql start
	
	$ psql -U guest -h 10.10.21.19 -d openpg
	


(6) 定期更新 (依照資料集的更新頻率做定期更新)
	$ crontab -e
	
		*/1 * * * * python {PATH}/conditional_Update.py
