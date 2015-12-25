(0) master/slave的PG設定

	$ sudo apt-get update

	$ sudo apt-get install postgresql postgresql-contrib postgresql-client

	$ sudo passwd postgres

		enter the password of postgres

	$ sudo -u postgres ssh-keygen

	$ ssh-copy-id $IP_address_of_opposite_server




(1) master環境設定 (multicorn, git, make, pycurl, 程式移到multicorn目錄)

	$ sudo python InstallOnMaster.py


	
(2) 多機PG設定

	$ sudo -u postgres python PGdeploy.py local_ip remote_ip




(3) 建立OpenPG框架

	$ python OPPGsetup.py local_ip remote_ip



(4) load open data from 台北市開放資料

	$ python GetOpenData.py

	

(5) open slave (on slave)

	$ vi /etc/postgresql/9.3/main/pg_hba.conf

		host    all             all             all            md5

	$ psql -U guest -h 10.10.21.19 -d openpg
