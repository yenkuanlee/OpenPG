# -*- coding: utf-8 -*-
from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.compat import unicode_
from .utils import log_to_postgres, WARNING, ERROR
from itertools import cycle
from datetime import datetime
import os
from . import ForeignDataWrapper
from logging import WARNING
import csv
import psycopg2

class FDWwrite(ForeignDataWrapper):

    _startup_cost = 10

    def __init__(self, options, columns):
        super(FDWwrite, self).__init__(options, columns)
	self.filename = options["filename"]
        self.delimiter = options.get("delimiter", "\t")
        self.quotechar = options.get("quotechar", '"')
        self.skip_header = int(options.get('skip_header', 0))

        self.columns = columns
        self.test_type = options.get('test_type', None)
        self.tx_hook = options.get('tx_hook', False)
        self._row_id_column = options.get('row_id_column',
                                          list(self.columns.keys())[0])
        log_to_postgres(str(sorted(options.items())))
        log_to_postgres(str(sorted([(key, column.type_name) for key, column in
                                    columns.items()])))
        for column in columns.values():
            if column.options:
                log_to_postgres('Column %s options: %s' %
                                (column.column_name, column.options))
        if self.test_type == 'logger':
            log_to_postgres("An error is about to occur", WARNING)
            log_to_postgres("An error occured", ERROR)
    

    def execute(self, quals, columns):
        with open(self.filename) as stream:
            reader = csv.reader(stream, delimiter=self.delimiter)
            count = 0
            checked = False
            for line in reader:
                if count >= self.skip_header:
                    if not checked:
                        # On first iteration, check if the lines are of the
                        # appropriate length
                        checked = True
                        if len(line) > len(self.columns):
                            log_to_postgres("There are more columns than "
                                            "defined in the table", WARNING)
                        if len(line) < len(self.columns):
                            log_to_postgres("There are less columns than "
                                            "defined in the table", WARNING)
                    yield line[:len(self.columns)]
                count += 1
        #log_to_postgres(str(sorted(quals)))
        #log_to_postgres(str(sorted(columns)))
        #if self.test_type == 'None':
        #    return None
        #elif self.test_type == 'iter_none':
        #    return [None, None]
        #else:
        #    return self._as_generator(quals, columns)
    def get_rel_size(self, quals, columns):
        if self.test_type == 'planner':
            return (10000000, len(columns) * 10)
        return (20, len(columns) * 10)
    def get_path_keys(self):
        if self.test_type == 'planner':
            return [(('test1',), 1)]
        return []

    def update(self, rowid, newvalues):
	if "更新時間" in newvalues:
		line_number = 0
		f = open(self.filename,'r')
		while True:
			line = f.readline()
			if not line: break
			line_number += 1
			if line.split("\t")[0]==rowid:break
		f.close()
		os.system("sed -i \""+str(line_number)+"s/\t2...-.*-.* .*:.*:.*08\t/\t"+str(newvalues['更新時間'])+"\t/g\" "+self.filename)
		conn = psycopg2.connect(database="openpg", user="postgres", password="123456", host="127.0.0.1", port="5432")
        	cur = conn.cursor()
        	cur.execute("REFRESH MATERIALIZED VIEW "+rowid+" ;")
        	conn.commit()
        	conn.close()
		#os.system("echo \""+str(rowid)+"\" >> /home/postgres/qqq.txt")


        if self.test_type == 'nowrite':
            super(FDWwrite, self).update(rowid, newvalues)
        log_to_postgres("UPDATING: %s with %s" % (
            rowid, sorted(newvalues.items())))
        if self.test_type == 'returning':
            for key in newvalues:
                newvalues[key] = "UPDATED: %s" % newvalues[key]
            return newvalues

    
    def delete(self, rowid):
	Dname = rowid+"\t"
	Dname = Dname.encode('utf-8')
	os.system("sed -i '/^"+Dname+"/d' "+self.filename)

	conn = psycopg2.connect(database="openpg", user="postgres", password="123456", host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("drop foreign table FT_"+rowid+" CASCADE;")
	conn.commit()
	conn.close()

	if self.test_type == 'nowrite':
            super(FDWwrite, self).delete(rowid)
        log_to_postgres("DELETING: %s" % rowid)

    def insert(self, values):
	new_line = values['tid']+"\t"+values['表格名稱']+"\t"+values['原始資料格式']+"\t"+values['欄位']+"\t"+values['網址']+"\t"+values['描述']+"\t"+values['更新時間']+"\t"+values['csv_skip_header']+"\t"+values['csv_delimiter']+"\t"+values['elem_tag']
	os.system("echo \""+(new_line).encode('utf-8').replace("\r","")+"\" >> "+ self.filename)
	sql_query = ""
	if values['原始資料格式']=="csv":
		sql_query = "create foreign table FT_"+(values['tid'])+"("+values['欄位'].replace("#"," ")+") server web_csv options (skip_header '"+values['csv_skip_header']+"', delimiter '"+values['csv_delimiter']+"',url '"+values['網址']+"');"
	if values['原始資料格式']=="xml":
		sql_query = "create foreign table FT_"+(values['tid'])+"("+values['欄位'].replace("#"," ")+") server Web_XML options (elem_tag '"+values['elem_tag']+"',url '"+values['網址']+"');"
	if values['原始資料格式']=="json":
		sql_query = "create foreign table FT_"+(values['tid'])+"("+values['欄位'].replace("#"," ")+") server Web_Json options (url '"+values['網址']+"');"
	conn = psycopg2.connect(database="openpg", user="postgres", password="123456", host="127.0.0.1", port="5432")
        cur = conn.cursor()
        cur.execute(sql_query.encode('utf-8'))
	cur.execute("CREATE MATERIALIZED VIEW "+values['tid']+" AS select * from FT_"+values['tid']+";")
        conn.commit()
        conn.close()
	#os.system("echo \""+sql_query+"\" >> /home/postgres/qqq.txt")
    @property
    def rowid_column(self):
        return self._row_id_column

    def begin(self, serializable):
        if self.tx_hook:
            log_to_postgres('BEGIN')

    def sub_begin(self, level):
        if self.tx_hook:
            log_to_postgres('SUBBEGIN')

    def sub_rollback(self, level):
        if self.tx_hook:
            log_to_postgres('SUBROLLBACK')

    def sub_commit(self, level):
        if self.tx_hook:
            log_to_postgres('SUBCOMMIT')

    def commit(self):
        if self.tx_hook:
            log_to_postgres('COMMIT')

    def pre_commit(self):
        if self.tx_hook:
            log_to_postgres('PRECOMMIT')

    def rollback(self):
        if self.tx_hook:
            log_to_postgres('ROLLBACK')

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        log_to_postgres("IMPORT %s FROM srv %s OPTIONS %s RESTRICTION: %s %s" %
                        (schema, srv_options, options, restriction_type,
                         restricts))
        tables = set([unicode_("imported_table_1"),
                      unicode_("imported_table_2"),
                      unicode_("imported_table_3")])
        if restriction_type == 'limit':
            tables = tables.intersection(set(restricts))
        elif restriction_type == 'except':
            tables = tables - set(restricts)
        rv = []
        for tname in sorted(list(tables)):
            table = TableDefinition(tname)
            nb_col = options.get('nb_col', 3)
            for col in range(nb_col):
                table.columns.append(
                    ColumnDefinition("col%s" % col,
                                     type_name="text",
                                     options={"option1": "value1"}))
            rv.append(table)
        return rv

