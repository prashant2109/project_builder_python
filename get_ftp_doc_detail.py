#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import datetime

class get_ftp_doc_detail(object):
    def __inti__(self):
        pass

    def get_details(self, project_id, url_id):
        #/var/www/html/WorkSpaceBuilder_DB_demo
        opath = "/var/www/html/WorkSpaceBuilder_DB_demo/ftpdata/%s/%s/" %(project_id, url_id)
        ret = {} 
        for (dirpath, dirnames, filenames) in os.walk(opath):
            if filenames:
                ret[dirpath] = []
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(filepath)
                cdtime = datetime.datetime.fromtimestamp(ctime)
                mdtime = datetime.datetime.fromtimestamp(mtime)
                adtime = datetime.datetime.fromtimestamp(atime)
                ret[dirpath].append({'docname':filename, 'time':str(cdtime)})
        return ret
        
        
