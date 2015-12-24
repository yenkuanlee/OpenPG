# -*- coding: utf-8 -*-
import os
from StringIO import StringIO
import pycurl
from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import WARNING
import csv
import chardet
import os
class WebCsvFdw(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(WebCsvFdw, self).__init__(fdw_options, fdw_columns)
        self.url = fdw_options["url"]
        self.delimiter = fdw_options.get("delimiter", ",")
        self.quotechar = fdw_options.get("quotechar", '"')
        self.skip_header = int(fdw_options.get('skip_header', 0))
        self.columns = fdw_columns

    def execute(self, quals, columns):
	os.system("echo \"123\" >> /home/hdc/yenkuanlee/qq.txt")
	url = self.url
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue()
	L =  content.split("\n")
	for i in range(len(L)):
		if not L[i]:
			L[i] = "NULL"
			continue
                if chardet.detect(L[i])['encoding']!='utf-8':
                        L[i]=L[i].decode('Big5','ignore').encode('utf-8')
        reader = csv.reader(L[:len(L)-1], delimiter=self.delimiter)
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
    def insert(self, values):
	#os.system("mkdir /tmp/KKK")
	self.connection.execute(self.table.insert(values=values))

