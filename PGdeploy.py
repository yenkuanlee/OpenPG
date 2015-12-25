import os
import sys
postgresql_conf = ""
postgresql_conf += "sed -i \"s/.*listen_addresses.*=.*/listen_addresses = '*'/g\" /etc/postgresql/9.3/main/postgresql.conf;"
postgresql_conf += "sed -i \"s/.*wal_level.*=.*/wal_level = 'hot_standby'/g\" /etc/postgresql/9.3/main/postgresql.conf;"
postgresql_conf += "sed -i \"s/.*archive_mode.*=.*/archive_mode = on/g\" /etc/postgresql/9.3/main/postgresql.conf;"
postgresql_conf += "sed -i \"s/.*archive_command.*=.*/archive_command = 'cd .'/g\" /etc/postgresql/9.3/main/postgresql.conf;"
postgresql_conf += "sed -i \"s/.*max_wal_senders.*=.*/max_wal_senders = 1/g\" /etc/postgresql/9.3/main/postgresql.conf;"
postgresql_conf += "sed -i \"s/.*hot_standby.*=.*/hot_standby = on/g\" /etc/postgresql/9.3/main/postgresql.conf;"

def MasterConf(remote_ip):
        os.system("psql -c \"DROP ROLE IF EXISTS guest\"")
        os.system("psql -c \"CREATE USER guest REPLICATION LOGIN CONNECTION LIMIT 100 ENCRYPTED PASSWORD 'guest';\"")
        os.system("sed -i \"s/^host    replication     guest     "+remote_ip+".32   md5.*//g\"  /etc/postgresql/9.3/main/pg_hba.conf")
        os.system("sed -i \"2a host    replication     guest     "+remote_ip+"/32   md5\" /etc/postgresql/9.3/main/pg_hba.conf")
        os.system(postgresql_conf)
        os.system("service postgresql restart")

def SlaveConf(local_ip,remote_ip):
        command = ""
        command += "service postgresql stop;"
        command += "sed -i \\\"s/^host    replication     guest     "+local_ip+".32   md5.*//g\\\"  /etc/postgresql/9.3/main/pg_hba.conf;"
        command += "sed -i \\\"2a host    replication     guest     "+local_ip+"/32   md5\\\" /etc/postgresql/9.3/main/pg_hba.conf;"
        command += postgresql_conf.replace("\"","\\\"")
        os.system("ssh "+remote_ip+" -t \""+command+"\"")

def Replicating_the_Initial_database(local_ip,remote_ip):
        os.system("psql -c \"select pg_start_backup('initial_backup');\"")
        os.system("rsync -cva --inplace --exclude=*pg_xlog* /var/lib/postgresql/9.3/main/ "+remote_ip+":/var/lib/postgresql/9.3/main/")
        os.system("psql -c \"select pg_stop_backup();\"")
        command = ""
        command += "rm -rf /var/lib/postgresql/9.3/main/recovery.conf;"
        command += "bash -c \\\"echo 'standby_mode = 'on'' > /var/lib/postgresql/9.3/main/recovery.conf\\\";"
        command += "sed -i \\\"s/on/'on'/g\\\" /var/lib/postgresql/9.3/main/recovery.conf;"
        command += "sed -i \\\"1a primary_conninfo = 'host="+local_ip+" port=5432 user=guest password=guest'\\\" /var/lib/postgresql/9.3/main/recovery.conf;"
        command += "sed -i \\\"2a trigger_file = '/tmp/postgresql.trigger.5432'\\\" /var/lib/postgresql/9.3/main/recovery.conf;"
        command += "service postgresql start;"
        os.system("ssh "+remote_ip+" -t \""+command+"\"")

def Restart(remote_ip):
        os.system("service postgresql stop;")
        os.system("ssh "+remote_ip+" -t \"service postgresql stop;\"")
        os.system("service postgresql start;")
        os.system("ssh "+remote_ip+" -t \"service postgresql start;\"")

MasterConf(sys.argv[2])
SlaveConf(sys.argv[1],sys.argv[2])
Replicating_the_Initial_database(sys.argv[1],sys.argv[2])
Restart(sys.argv[2])
