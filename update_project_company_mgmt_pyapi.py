#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime,sqlite3, MySQLdb
from urllib import unquote
#import numpy as np
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config
from urllib import unquote
from urlparse import urlparse
class PYAPI():
    def __init__(self):
        pypath    = os.path.dirname(os.path.abspath(__file__))
        self.dateformat = {
                            'www.businesswire.com'      : ['%Y-%m-%d %H:%M:%S', '%B %s, %Y %H:%M:%S'], #[April 24, 2019 12:44:29]
                            'press.aboutamazon.com'     : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'www.sec.gov'               : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'pressroom.aboutschwab.com' : ['%m/%d/%y %H:%M %p %Z'],
                            }
        self.month_map  = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        pass

    def read_txt_from_server(self, file_path, ip):
        pswd = 'tas123'
        if ip == '172.16.20.52':
            pswd = 'work@999'
        import paramiko
        ssh_client_obj = paramiko.SSHClient()
        ssh_client_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client_obj.connect(ip, username='root', password=)
        sftp_client = ssh_client_obj.open_sftp()
        another_server_file = sftp_client.open(file_path)
        txt_data = another_server_file.readlines()
        another_server_file.close()
        ssh_client_obj.close()
        return txt_data
    
    def update_insert_uploaded_doc_info(self, ijson):
        docid_lst        = ijson['doc_ids']
        project_id       = str(ijson['project_id'])
        cid              = str(ijson['crid'])
        doc_type         = str(ijson['doc_type'])
        user_name        = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
            
        chk_read = """ SELECT project_id, company_row_id, doc_id FROM document_mgmt WHERE project_id='%s' AND company_row_id='%s' AND user_name='%s' """%(project_id, cid, user_name)
        m_cur.execute(chk_read)
        t_info = m_cur.fetchall()       
            
        info_chk_dct = {}
        for rw in t_info:
            pid, crid, dcid = str(rw)
            info_chk_dct[(info_chk_dct)] = 1

        insert_rows = []
        update_rows = []
        
        for dc_info in docid_lst:
            dc, meta = dc_info['doc_id'],  dc_info['meta_data']                   
            if (project_id, company_row_id, dc) in info_chk_dct:
                update_rows.append((meta, project_id, company_row_id, dc, user_name))                
            elif (project_id, company_row_id, dc) not in info_chk_dct:
                if doc_type == 'PDF':
                    dc_page_path = '/var/www/html/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/pdf_total_pages'%(project_id, dc) 
                    nop = str(self.read_txt_from_server(dc_page_path)[0])
                elif doc_type == 'HTML':
                    nop = '1' 
                insert_rows.append((project_id, company_row_id, meta, dc, 'N', nop, user_name))
        
        if insert_rows:
            ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status, no_of_pages, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s)  """
    
            m_cur.execute(ins_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE document_mgmt SET meta_data=%s WHERE project_id=%s AND company_row_id=%s AND doc_id=%s AND user_name=%s  """
            m_cur.execute(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def get_url_stats(self, ijson):
        project_id  = ijson.get("project_id","Amazon")
        user_id     = ijson.get("user_id","tas")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        sql         = "select project_id, url_id, urlname, status from project_url_mgmt where project_id = '%s'"
        cur.execute(sql % (project_id))
        res         = cur.fetchall()
        stats       = {}
        #{"src_date_time":"7/16/19 5:45 am PDT","Headline":"Schwab Reports Net Income of $937 Million, Up 8%, Posting the Strongest Second Quarter in Company History","File Description":"The Charles Schwab Corporation announced today that its net income for the second quarter of 2019 was $937 million, down 3% from $964 million for the prior quarter, and up 8% from $866 million for the second quarter of 2018. Net income for the six months ended June 30, 2019 was a record $1.9 billion, up 15% from the year-earlier period.\n\n\n\n\n\n \n\n\n\n\n \n\n\n\n\nThree Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n \n\n\n\n\nSix Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n\n\nFinancial Highlights\n\n\n\n\n \n\n\n\n\n2019\n\n\n\n\n  more...","linkid":"219","link":"https://pressroom.aboutschwab.com/press-release/corporate-and-financial-news/schwab-reports-net-income-937-million-8-posting-strongest"} 
        done_d  = {}
        year_d  = {}
        url_d   = {}
        overall = {}
        sn  = 1
        recent  = []
        done_url    = {}
        for r in res:
            project_id, url_id, urlname, status = r
            sql = "select document_id, urlname, meta_data, process_time from scheduler_download_common_new_%s_%s.link_mgmt_meta_data where active_status='Y'"%(project_id, url_id)
            cur.execute(sql)
            tmpres  = cur.fetchall()
            #stats.setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
            tmplst  = []
            udata = urlparse(urlname)
            domain  = udata.netloc.split('/')[-1]
            if (domain, udata.path) in done_url:
                url_id  = done_url[(domain, udata.path)]
            else:
                done_url[(domain, udata.path)]   = url_id
            for tmpr in tmpres:
                document_id, urlname, meta_data, process_time   = tmpr
                document_id = str(document_id)
                meta_data       = json.loads(unquote(meta_data).decode('utf8'))
                #print (url_id, urlname, meta_data)
                #print (meta_data["src_date_time"], self.dateformat[domain])
                src_date_time   = ''
                for frmt in self.dateformat[domain]:
                    try:
                        src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"]), frmt)
                    except:
                        try:
                            src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"].split()[0]), frmt.split()[0])    
                        except:pass
                    if src_date_time:break
                if not src_date_time:
                    print 'ERROR ', (url_id, meta_data["src_date_time"], domain, self.dateformat[domain])
                    continue
                tmplst.append((document_id, urlname, process_time, meta_data, src_date_time))
            urlname = urlname.decode("iso-8859-1")
            if isinstance(urlname, unicode):
                urlname = urlname.encode('utf-8')
            url_d[url_id]   = {'n':domain, 'link':urlname, 's':status, 'sn':sn, 'uinfo':{}, 'info':udata.path}        
            sn  += 1
            tmplst.sort(key=lambda x:x[4], reverse=True)
            stats   = {'total':{}, 'processed':{}, 'pending':{}, 'rejected':{}}
            for ii, tmpr in enumerate(tmplst):
                document_id, urlname, process_time, meta_data, src_date_time    = tmpr
                year            = int(src_date_time.strftime('%Y'))
                if year < 2000:continue
                urlname = urlname.decode("iso-8859-1")
                if isinstance(urlname, unicode):
                    urlname = urlname.encode('utf-8')
                if ii == 0:
                    recent.append({'n':domain, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 'count':1, 'd':str(document_id), 'link':urlname})
                month           = src_date_time.strftime('%b')
                Headline        = meta_data['Headline']
                #desc            = meta_data['File Description']
                stats['total'][document_id]   = 1
                year_d.setdefault(year, {}).setdefault(month, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d[year][month][url_id]['total']    += 1
                year_d.setdefault('Overall', {}).setdefault(year, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d['Overall'][year][url_id]['total']    += 1
                done_sts    = 'N'
                if done_d.get(str(document_id)) == 'Y':
                    done_sts    = 'Y'
                    stats['processed'][document_id]   = 1
                    year_d[year][month][url_id]['processed']        += 1
                    year_d['Overall'][year][url_id]['processed']    += 1
                elif done_d.get(str(document_id)) == 'R':
                    stats['rejected'][document_id]   = 1
                    year_d[year][month][url_id]['rejected']        += 1
                    year_d['Overall'][year][url_id]['rejected']    += 1
                    done_sts    = 'R'
                else:
                    stats['pending'][document_id]   = 1
                    year_d[year][month][url_id]['pending']        += 1
                    year_d['Overall'][year][url_id]['pending']    += 1
                    done_sts    = 'P'
                #Headline    = Headline.decode("iso-8859-1")
                if isinstance(Headline, unicode):
                    Headline = Headline.encode('utf-8')
                dtype   = 'Link'
                if urlname.split('?')[0].split('.')[-1].lower() == 'pdf':
                    dtype   = 'PDF'
                url_d[url_id]['uinfo'][str(document_id)]    = {'n':Headline, 'link':urlname, 'sn':ii+1, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 's':done_sts, 'dtype':dtype}
            if not url_d[url_id]['uinfo']:
                url_d[url_id]['s']  = 'N'
            if url_d[url_id]['s']  == 'Y':
                url_d[url_id]['s'] = 'success'
            elif url_d[url_id]['s']  == 'N':
                url_d[url_id]['s'] = 'danger'
            total   = len(stats['total'].keys())
            pending = len(stats['pending'].keys())
            processed = len(stats['processed'].keys())
            rejected = len(stats['rejected'].keys())
            pending_p       = 0
            processed_p     = 0
            rejected_p      = 0
            if total:
                pending_p   = int((float(pending)/total)*100)
                processed_p   = int((float(processed)/total)*100)
                rejected_p   = int((float(rejected)/total)*100)
            stats['pending_c']  = pending
            stats['pending_p']  = pending_p
            stats['processed_c']  = processed
            stats['processed_p']  = processed_p
            stats['rejected_c']  = rejected
            stats['rejected_p']  = rejected_p
            url_d[url_id]['stats']  = stats
        gdata   = []
        years   = filter(lambda x:x != 'Overall', year_d.keys())
        years.sort(reverse=True)
        years   = ['Overall']+years
        for y in years[:]:
            months  = self.month_map
            if y == 'Overall':
                months  = year_d[y].keys()
                months.sort(reverse=True)
            dd  = {'categories':months, 'series':[], 'n':y}
            for urlid in url_d.keys():
                tdd  = {'name':url_d[urlid]['n'], 'data':[]}
                for m in months:
                    tdd['data'].append(year_d[y].get(m, {}).get(urlid, {}).get('total', 0))
                dd['series'].append(tdd)
            gdata.append(dd)
        u_ar    = []
        for urlid in url_d.keys():
            u_ar.append(url_d[urlid])
            u_ar[-1]['uid'] = urlid
                
        return [{'message':'done','data':u_ar, 'ysts':gdata, 'recent':recent}]

    def dinsert_demo_doc(self, ijson):
        dbname =  ''
        projectid = ''
        dbinfo  = copy.deepcopy(config.Config.dbinfo)
        dbinfo['host']  = '172.16.20.10'
        dbinfo['db']    = ijson['dbname']
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        pass

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def insert_demo_doc(self, ijson):
        db_name            = ijson["db_name"]
        project_id   = ijson["project_id"]
        batch_dct    = ijson["batch_dct"]
        
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        batch_str = ', '.join(["'"+str(e)+"'" for e in batch_dct.keys()])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE batch in (%s)  """%(batch_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            try:
                get_cid = batch_dct[str(batch)]
            except:get_cid = "2"
            data_rows.append((str(project_id), str(get_cid), str(meta_data), str(doc_id), 'Y'))  

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  

    def insert_demo_doc_no_batch(self):
        #sys.exit()
        db_name  = 'IS_Agreements' #'FactSheet_Tree'        #ijson["db_name"]
        project_id =  '69'            #ijson["project_id"]
        doc_ids  =  map(str, range(3, 13)) #   ['88', '87', '102', '103', '104', '107', '108', '109', '110']             #ijson["doc_lst"]
        company_row_id = '1' #"9" 

        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y', 'demo'))  

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status, user_name) VALUES(%s, %s, %s, %s, %s, %s)  """     
        print data_rows
        #sys.exit()
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    
    def add_companies(self, path):
        fin = open(path, 'r')
        lines   = fin.readlines()
        fin.close()
        p_d = {}
        doc_d   = {}
        for line in lines:
            line    = map(lambda x:x.strip(), line.split('\t'))
            if len(line) < 4:continue
            project_id, project_name, company_name, doc_id    = line
            doc_d.setdefault(project_id, {})[doc_id]   = 1
            project_id  = project_id.strip()
            p_d.setdefault(project_id, {}).setdefault(project_name, {}).setdefault(company_name, {})[doc_id]    = 1
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql = "select project_id, project_name from project_mgmt"
        m_cur.execute(sql)
        res = m_cur.fetchall()
        sql = "select row_id, company_display_name from company_mgmt"
        m_cur.execute(sql)
        res1 = m_cur.fetchall()
        sql = "select project_id, company_row_id from project_company_mgmt"
        m_cur.execute(sql)
        res2 = m_cur.fetchall()
        sql = "select project_id, company_row_id, doc_id from document_mgmt"
        m_cur.execute(sql)
        res3 = m_cur.fetchall()
        m_cur.close()
        m_conn.close()
        e_p_d   = {}
        
        for r in res:
            project_id, project_name    = r
            project_name    = ' '.join(project_name.lower().split())
            e_p_d[project_name]   = project_id
            e_p_d[(project_id, 'P')]   =  project_name
        c_p_d   = {}
        for r in res1:
            c_id, c_name    = r
            c_name          = ' '.join(c_name.lower().split())
            c_p_d[c_name]   = c_id
        proj_comp_d = {}
        for r in res2:
            p_id, c_id    = r
            proj_comp_d[(p_id, str(c_id))] = 1
        proj_comp_doc_d = {}
        for r in res3:
            p_id, c_id, doc_id    = r
            proj_comp_doc_d.setdefault((p_id, str(c_id)), {})[str(doc_id)] = 1

        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'WorkSpaceDb_DB'] 
        p_conn, p_cur = self.mysql_connection(db_data_lst)
        sql = "select ProjectID, ProjectCode from ProjectMaster"
        p_cur.execute(sql)
        res = p_cur.fetchall()
        dbname_d    = {}
        for r in res:
            ProjectID, ProjectCode  = r 
            dbname_d[str(ProjectID)]    = ProjectCode
        

        u_ar        = []
        ci_ar       = []
        p_cp_ar     = []
        doc_i_ar    = []
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        for pid , pinfo in p_d.items():
            #sys.exit()
            if pid.lower() in ['', 'new', '-1']:
                continue
            docids  = doc_d[pid].keys()
            db_data_lst = ['172.16.20.10', 'root', 'tas123', dbname_d[pid]] 
            p_conn, p_cur = self.mysql_connection(db_data_lst)
            read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(', '.join(docids))                
            
            p_cur.execute(read_qry)
            t_data = p_cur.fetchall()
            p_conn.close()
            data_rows = {}
            print read_qry
            for row in t_data:
                doc_id, batch, doc_name, doc_type, meta_data = row
                print [doc_id, batch, doc_name]
                data_rows[str(doc_id)] = (meta_data, doc_type, doc_name)
            if (pid, 'P') not in e_p_d:
                m_cur.execute("insert into project_mgmt(project_id, project_name, db_name, user_name)values('%s','%s','%s','%s')"%(pid, pinfo.keys()[0], dbname_d[pid], 'demo'))
                m_conn.commit()
            if 1:
                for cname, cinfo in pinfo[pinfo.keys()[0]].items():
                    tmpcname    = ' '.join(cname.lower().split())
                    tmpcname1    = ''.join(cname.split())
                    if tmpcname not in c_p_d:
                        ins_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name) VALUES('%s', '%s', '%s', '%s'); """%(tmpcname1, cname, '{}', 'demo')
                        m_cur.execute(ins_stmt)
                        m_conn.commit()
                 
                        read_qry = """ SELECT LAST_INSERT_ID(); """
                        m_cur.execute(read_qry)
                        crid   =  str(m_cur.fetchone()[0])
                        c_p_d[tmpcname] = crid
                    else:
                        crid    = str(c_p_d[tmpcname])
                    if(pid, crid) not in proj_comp_d:
                        p_cp_ar.append((pid, crid, 'demo'))
                    for doc_id in cinfo.keys():
                        if doc_id not in proj_comp_doc_d.get((pid, crid), {}):
                            dc_page_path = '/var/www/html/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/pdf_total_pages'%(pid, doc_id) 
                            txt_pg_info = '1'
                            try:
                                txt_pg_info = self.read_txt_from_server(dc_page_path)
                                txt_pg_info = str(txt_pg_info[0])
                            except:continue
                            print [doc_id]
                            meta_data, doc_type, doc_name   = data_rows[doc_id]
                            meta_data   = eval(meta_data)
                            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
                            doc_i_ar.append((pid, crid, doc_id, str(meta_data), txt_pg_info, 'Y'))
        m_cur.executemany('insert into document_mgmt(project_id, company_row_id, doc_id, meta_data, no_of_pages, status)values(%s,%s,%s, %s,%s,%s)', doc_i_ar)
        m_cur.executemany('insert into project_company_mgmt(project_id, company_row_id, user_name)values(%s,%s,%s)', p_cp_ar)
        m_conn.commit()
        m_conn.close() 

    def insert_demo_doc_p1(self):
        #sys.exit()
        project_id =  'FE'            #ijson["project_id"]
        doc_ids  = [''] #['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44']
        company_row_id = '6' #"9" 
        
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%('Westjetairlinesltd', '1')
        m_conn = sqlite3.connect(db_path)
        m_cur  = m_conn.cursor()
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids]) 
        read_qry = """  SELECT doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to FROM company_meta_info WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to = row
            meta_data = {'doc_type':document_type, 'period':period, 'reporting_year':reporting_year, 'doc_name':doc_name, 'doc_release_date':doc_release_date, 'doc_from':doc_from, 'doc_to':doc_to}
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y'))  
        
        #for k in data_rows: 
        #    print k
        #    print 
        #sys.exit()
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    
    def update_document_mgmt(self):
        # http://172.16.20.10:2222/WorkSpaceBuilder_DB/39/1/pdata/docs/20/pdf_total_pages
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        read_qry = """ SELECT project_id, doc_id, meta_data FROM document_mgmt WHERE project_id not in ('FE'); """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        dc_p_lst = []
        for row in t_data:
            project_id, doc_id = str(row[0]), str(row[1])
            meta_data = eval(row[2])
            doc_type = meta_data.get('doc_type', '') 
            if not doc_type:continue
            if doc_type == 'PDF':
                dc_page_path = '/var/www/html/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/pdf_total_pages'%(project_id, doc_id) 
                print dc_page_path
                try:
                    txt_pg_info = self.read_txt_from_server(dc_page_path)
                except:continue
                dc_p_lst.append((str(txt_pg_info[0]), project_id, doc_id))
            elif doc_type == 'HTML':
                dc_p_lst.append(('1', project_id, doc_id))
                
        update_stmt =  """UPDATE document_mgmt SET no_of_pages=%s WHERE project_id=%s AND doc_id=%s"""
        m_cur.executemany(update_stmt, dc_p_lst)
        m_conn.commit()
        m_conn.close()
        return 

    def update_doc_htmls(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        read_qry = """ SELECT project_id, doc_id, meta_data FROM document_mgmt;  """         
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        doc_data_dct = {}
        for row in t_data:
            project_id, doc_id, meta_data = str(row[0]), str(row[1]), eval(row[2])
            doc_type = meta_data.get('doc_type', '')
            if doc_type == 'HTML':
                doc_html_path  = '/var/www/html/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/html'%(project_id, doc_id)
                if not os.path.exists(doc_html_path):
                    cmd_dir = 'mkidr -p %s'%(doc_html_path)
                    print cmd_dir
                    ##os.system(cmd_dir)
                    ip_path = '/var/www/html/sltInputPath/%s/1/pdata/docs/%s/html/%s_body.html'%(project_id, doc_id, doc_id)
                    cmd_cp = 'cp %s %s'%(ip_path, doc_html_path)
                    print cmd_cp, '\n'
                    ##os.system(cmd_cp)
        return 

    def update_document_meta_data(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        read_qry = """ SELECT row_id, meta_data FROM document_mgmt WHERE project_id='FE'  ;  """         
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
    
if __name__ == '__main__':
    obj = PYAPI()
    ############################################################
    #ijson = {"db_name": "company_events", "project_id":"39", "batch_dct":{"Amazon":"1", "American Airlines":"4", "GAp Inc":"2", "JP Morgan":"3"}}
    #obj.insert_demo_doc(ijson)
    #obj.insert_demo_doc_no_batch()
    #obj.add_companies(sys.argv[1]) # insert data into company_mgmt and project_company_mgmt
    #obj.insert_demo_doc_p1() # add model_no 1 ; doc_id info into document_mgmt
    #obj.update_document_mgmt() # update page number, project id wise
    #obj.update_doc_htmls() # update page number, project id wise
    ############################################################
