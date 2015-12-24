# -*- coding: utf-8 -*-
import os
from StringIO import StringIO
import pycurl
from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import WARNING
import json
import chardet
from collections import OrderedDict
class WebJson(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(WebJson, self).__init__(fdw_options, fdw_columns)
        self.url = fdw_options["url"]
        self.columns = fdw_columns

    def execute(self, quals, columns):
	url = self.url
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue()

	j = json.loads(content, object_pairs_hook=OrderedDict)
        for x in j:
                line=list()
                for y in j[0].keys():
			if not x[y]:
				line.append("")
				continue
                        line.append(x[y].replace("\n","").replace("\r",""))
                yield line[:len(self.columns)]

