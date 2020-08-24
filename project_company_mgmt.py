import os, sys, MySQLdb, datetime
import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class  PC_mgmg(object):

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
    
    def insert_project_company_mgmt(self, ijson):
        
        info_lst = ijson['pc_info']
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
         
        #######################################################
        crt_stmt_p = """  CREATE TABLE IF NOT EXISTS project_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL, project_name VARCHAR(256) DEFAULT NULL, description TEXT DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, updated_user VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL, update_time VARCHAR(256) DEFAULT NULL)   """
        m_cur.execute(crt_stmt_p)
    
        crt_stmt_c = """  CREATE TABLE IF NOT EXISTS company_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, ,company_id VARCHAR(256) DEFAULT NULL, company_name VARCHAR(256) DEFAULT NULL, meta_data TEXT DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, updated_user VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL, update_time VARCHAR(256) DEFAULT NULL)   """
        m_cur.execute(crt_stmt_c)
        
        crt_stmt_pc = """  CREATE TABLE IF NOT EXISTS project_company_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL , company_id VARCHAR(256) DEFAULT NULL,  user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL)   """
        m_cur.execute(crt_stmt_pc)
        m_conn.commit() 
    
        read_p = """ SELECT project_id  FROM project_mgmt """
        m_cur.execute(read_p)
        p_data = {str(rw[0]):1 for rw in m_cur.fetchall()}
        
        read_c = """ SELECT company_id  FROM company_mgmt  """
        m_cur.execute(read_c)
        c_data = {str(rw[0]):1 for rw in m_cur.fetchall()}
        
        read_pc = """ SELECT project_id, company_id  FROM project_company_mgmt  """
        m_cur.execute(read_c)
        c_data = {(str(rw[0]), str(rw[1])):1 for rw in m_cur.fetchall()}

        insert_rows_p = []
        update_rows_p = [] 
        insert_rows_c = []
        update_rows_c = [] 
        for row_dct in info_lst:
            project_id, project_name, company_id, company_name, description, meta_data, user_name = row_dct['project_id'], row_dct['project_name'], row_dct['company_id'], row_dct['company_name'], row_dct['description'], row_dct['meta_data'], row_dct['user']
            if str(project_id) in p_data:
                update_rows_p.append((project_name, desc, user, str(datetime.datetime.now()), project_id))    
            else:
                insert_rows_p.append((project_id, project_name, description, user_name, str(datetime.datetime.now()))) 

            if str(company_id) in c_data:
                update_rows_c.append((company_name, meta_data, user, str(datetime.datetime.now()), project_id))    
            else:
                insert_rows_c.append((company_id, company_name, meta_data, user_name, str(datetime.datetime.now()))) 


        insert_stmt_p = """ INSERT INTO project_mgmt(project_id, project_name, description, user_name, updated_user, insert_time, update_time) VALUES(%s, %s, %s, %s, %s, %s, %s)  """
        m_cur.executemany(insert_stmt_p, insert_rows_p) 
        update_stmt_p = """ UPDATE project_mgmt SET description=%s, updated_user=%s, update_time=%s WHERE project_id=%s AND project_name=%s """
        m_cur.executemany(update_stmt_p, update_rows_p)
    
        #######################################################
        crt_stmt_c = """  CREATE TABLE IF NOT EXISTS company_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, ,company_id VARCHAR(256) DEFAULT NULL, company_name VARCHAR(256) DEFAULT NULL, meta_data TEXT DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, updated_user VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL, update_time VARCHAR(256) DEFAULT NULL)   """
        m_cur.execute(crt_stmt_c)

        insert_stmt_c = """ INSERT INTO company_mgmt(company_id, company_name, meta_data, user_name, updated_user, insert_time, update_time) VALUES(%s, %s, %s, %s, %s, %s)  """
        m_cur.executemany(insert_stmt_c, insert_rows_c) 
        update_stmt_c = """ UPDATE company_mgmt SET meta_data=%s, updated_user=%s, update_time=%s WHERE company_id=%s AND company_name=%s """
        m_cur.executemany(update_stmt_c, update_rows_c)

        #######################################################

        crt_stmt_pc = """  CREATE TABLE IF NOT EXISTS project_company_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL , company_id VARCHAR(256) DEFAULT NULL,  user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL)   """
        m_cur.execute(crt_stmt_pc)

        insert_stmt_pc = """ INSERT INTO project_company_mgmt(project_id, company_id, user_name, insert_time) VALUES(%s, %s, %s, %s)  """
        m_cur.executemany(insert_stmt_pc, insert_rows_pc) 

    def read_hfs_info(self, m_cur, m_conn):
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE project_id='HFS' """
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchone()
        p_id, p_n, desc, db_n = dt_p
        project_id, cid_tup = p_id, (p_n, desc, db_n)
        import pr_pyapi as pyf
        p_Obj = pyf.PYAPI()
        xl_data = p_Obj.read_xl_using_openpyxl()
        #p_dct = {''} 
        # ['21', 'HSBC', 'HGIF Brazil Bond AC - May 2019 HK-EN.pdf', 'Fixed Income', 'HSBC_Fixed Income', 'May']
        # {'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'url_id':ul_id}
        p_dct = {'project_id': project_id, 'project_name':cid_tup[0], 'desc':cid_tup[1] , 'info':[], 'db_name':cid_tup[2]}
        c_inf_dct = {}
        for row in xl_data['Sheet2'][1:]:
            doc_id, c_name, doc_name, c_type, typ, month = map(str, row[1:7])
            company_id = c_name +'-'+ ''.join(c_type.split())
            company_name = c_name +'-'+ c_type
            doc_dct = {'d':doc_id, 'status':'Y', 'nop':0, 'doc_name':doc_name} 
            c_inf_dct.setdefault((company_id, company_name), []).append(doc_dct)
        for cid_tp, dc_lst in c_inf_dct.iteritems():
            print dc_lst
            dc_lst.sort(key=lambda x:int(x['d']))
            c_dct =  {'company_name':cid_tp[1], 'company_id':cid_tp[0],'user':'', 'info':dc_lst, 'crid':'', 'model_id':'', 'rc_id':'', 'rc_user':'tas', 'url_id':''} 
            p_dct['info'].append(c_dct)
        return p_dct

    def read_user_wise_project_info(self, user_id, m_conn, m_cur):
        read_qry = """ SELECT project_id, company_row_id FROM user_configuration_info WHERE assigned_user_id='%s' """%(user_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        project_comp_dct = {}
        for row in t_data:
            project_id, company_row_id = row
            project_comp_dct.setdefault(project_id, {})[company_row_id] = 1
        return project_comp_dct

    def rm_user_filter_read_project_comapny_mgmt(self, ijson):
        user_name = ijson.get('user', 'demo')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        project_comp_dct = self.read_user_wise_project_info(user_name, m_conn, m_cur)
        if not project_comp_dct:
            return [{'message':'done', 'data':[]}]
        
        pc_info_dct = {}         
        for prj, cm_dct in project_comp_dct.iteritems():
            cmpid_str = ', '.join({"'"+str(e)+"'" for e in cm_dct})
            read_pc = """ SELECT project_id, company_row_id, user_name, meta_data, url_id FROM project_company_mgmt WHERE project_id='%s' AND company_row_id in (%s); """%(prj, cmpid_str)
            m_cur.execute(read_pc)
            pc_data = m_cur.fetchall()
        
            for row in pc_data:
                project_id, company_id, user_name = map(str, row[:-2])
                try:
                    meta_data = eval(row[-2])
                except:meta_data = {}
                url_id = row[-1]
                if not url_id:  
                    url_id = ''
                pc_info_dct.setdefault(project_id, {})[company_id] = (user_name, meta_data, str(url_id))
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE project_id in (%s) """%(pid_str)
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        res_lst = []
        for pid, cid_dct in p_map_dct.iteritems():
            cid_dct = pc_info_dct.get(pid, {})
            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            if pid == 'HFS':
                print 'HERE', pid
                continue 
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            if cid_dct:
                cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s) """%(cid_str)
                m_cur.execute(cid_dct_qry)
                c_data = m_cur.fetchall()
            else:
                c_data=[]
            p_dct = {'project_id': pid, 'project_name': p_name, 'desc': description, 'info':[], 'db_name':db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' AND company_row_id in (%s) """%(pid, cid_str)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            dc_data = list(dc_data)
            dc_data.sort(key=lambda x:int(x[1]))
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                if not dmeta_data:
                    dmeta_data = {}
                tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
            for cid, us_nm in cid_dct.iteritems():
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                m_id_tup = pc_info_dct[pid][cid]
                m_id  = m_id_tup[1].get("model_id", pid)
                rc_id = m_id_tup[1].get("rc_id", pid)
                ul_id = cid #m_id_tup[2]
                p_dct['info'].append({'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'monitor_id':ul_id})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        p_dct['info'][-1][k]    =v
            get_p_dct_info = p_dct['info'] 
            get_p_dct_info.sort(key=lambda x:x['company_name'])
            p_dct['info'] = get_p_dct_info 
            res_lst.append(p_dct)
        #p_inf = self.read_hfs_info(m_cur, m_conn)
        #res_lst.append(p_inf)
        m_conn.close()
        return [{'message':'done', 'data':res_lst}]

    def read_project_comapny_mgmt(self, ijson):
        user_name = ijson.get('user', 'demo')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        read_pc = """ SELECT project_id, company_row_id, user_name, meta_data, url_id FROM project_company_mgmt; """
        m_cur.execute(read_pc)
        pc_data = m_cur.fetchall()
    
        pc_info_dct = {}         
        for row in pc_data:
            project_id, company_id, user_name = map(str, row[:-2])
            try:
                meta_data = eval(row[-2])
            except:meta_data = {}
            url_id = row[-1]
            if not url_id:  
                url_id = ''
            pc_info_dct.setdefault(project_id, {})[company_id] = (user_name, meta_data, str(url_id))
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt; """
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        res_lst = []
        for pid, cid_dct in p_map_dct.iteritems():
            cid_dct = pc_info_dct.get(pid, {})
            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            if pid == 'HFS':
                #call a function            
                #p_dt = self.read_hfs_info(pid, (p_name, description, db_name))
                #res_lst.append(p_dt)
                print 'HERE', pid
                continue 
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            if cid_dct:
                cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s); """%(cid_str)
                m_cur.execute(cid_dct_qry)
                c_data = m_cur.fetchall()
            else:
                c_data=[]
            p_dct = {'project_id': pid, 'project_name': p_name, 'desc': description, 'info':[], 'db_name':db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' """%(pid)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            dc_data = list(dc_data)
            dc_data.sort(key=lambda x:int(x[1]))
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                '''
                if pid == 'FE':
                    if crd == '7':
                        if did not in ('3593', '3406'):continue
                '''
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                if not dmeta_data:
                    dmeta_data = {}
                tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
            for cid, us_nm in cid_dct.iteritems():
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                m_id_tup = pc_info_dct[pid][cid]
                m_id  = m_id_tup[1].get("model_id", pid)
                rc_id = m_id_tup[1].get("rc_id", pid)
                ul_id = cid #m_id_tup[2]
                p_dct['info'].append({'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'monitor_id':ul_id})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        p_dct['info'][-1][k]    =v
            get_p_dct_info = p_dct['info'] 
            get_p_dct_info.sort(key=lambda x:x['company_name'])
            p_dct['info'] = get_p_dct_info 
            res_lst.append(p_dct)
        #p_inf = self.read_hfs_info(m_cur, m_conn)
        #res_lst.append(p_inf)
        m_conn.close()
        return [{'message':'done', 'data':res_lst}]
                                    
    def cppread_project_comapny_mgmt(self, ijson):
        user_name = ijson.get('user', 'demo')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        read_pc = """ SELECT project_id, company_row_id, user_name, meta_data, url_id FROM project_company_mgmt WHERE user_name='%s' ; """%(user_name)
        m_cur.execute(read_pc)
        pc_data = m_cur.fetchall()
    
        pc_info_dct = {}         
        for row in pc_data:
            project_id, company_id, user_name = map(str, row[:-2])
            try:
                meta_data = eval(row[-2])
            except:meta_data = {}
            url_id = row[-1]
            if not url_id:  
                url_id = ''
            pc_info_dct.setdefault(project_id, {})[company_id] = (user_name, meta_data, str(url_id))
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE user_name='%s' """%(user_name)
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        res_lst = []
        for pid, cid_dct in p_map_dct.iteritems():
            cid_dct = pc_info_dct.get(pid, {})
            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            if pid == 'HFS':
                #call a function            
                #p_dt = self.read_hfs_info(pid, (p_name, description, db_name))
                #res_lst.append(p_dt)
                print 'HERE', pid
                continue 
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            if cid_dct:
                cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s) AND user_name='%s'  """%(cid_str, user_name)
                m_cur.execute(cid_dct_qry)
                c_data = m_cur.fetchall()
            else:
                c_data=[]
            p_dct = {'project_id': pid, 'project_name': p_name, 'desc': description, 'info':[], 'db_name':db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' AND user_name='%s' """%(pid, user_name)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            dc_data = list(dc_data)
            dc_data.sort(key=lambda x:int(x[1]))
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                '''
                if pid == 'FE':
                    if crd == '7':
                        if did not in ('3593', '3406'):continue
                '''
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                if not dmeta_data:
                    dmeta_data = {}
                tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
            for cid, us_nm in cid_dct.iteritems():
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                m_id_tup = pc_info_dct[pid][cid]
                m_id  = m_id_tup[1].get("model_id", pid)
                rc_id = m_id_tup[1].get("rc_id", pid)
                ul_id = cid #m_id_tup[2]
                p_dct['info'].append({'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'monitor_id':ul_id})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        p_dct['info'][-1][k]    =v
            get_p_dct_info = p_dct['info'] 
            get_p_dct_info.sort(key=lambda x:x['company_name'])
            p_dct['info'] = get_p_dct_info 
            res_lst.append(p_dct)
        #p_inf = self.read_hfs_info(m_cur, m_conn)
        #res_lst.append(p_inf)
        m_conn.close()
        return [{'message':'done', 'data':res_lst}]

    def org_only_doc_info(self, ijson):
        prj_id = str(ijson['project_id'])
        cmp_rid = str(ijson['crid'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        read_pc = """ SELECT project_id, company_row_id, user_name, meta_data, url_id FROM project_company_mgmt WHERE project_id='%s' ; """%(prj_id)
        m_cur.execute(read_pc)
        pc_data = m_cur.fetchall()
    
        pc_info_dct = {}         
        for row in pc_data:
            project_id, company_id, user_name = map(str, row[:-2])
            try:
                meta_data = eval(row[-2])
            except:meta_data = {}
            url_id = row[-1]
            if not url_id:  
                url_id = ''
            pc_info_dct.setdefault(project_id, {})[company_id] = (user_name, meta_data, str(url_id))
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE project_id in (%s) """%(pid_str)
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        res_lst = []
        for pid, cid_dct in pc_info_dct.iteritems():

            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            if pid == 'HFS':
                #call a function            
                #p_dt = self.read_hfs_info(pid, (p_name, description, db_name))
                #res_lst.append(p_dt)
                print 'HERE', pid
                continue 
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s)  """%(cmp_rid)
            m_cur.execute(cid_dct_qry)
            c_data = m_cur.fetchall()
            p_dct = {'project_id': pid, 'project_name': p_name, 'desc': description, 'info':[], 'db_name':db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' AND company_row_id='%s'  """%(pid, cmp_rid)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            dc_data = list(dc_data)
            dc_data.sort(key=lambda x:int(x[1]))
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                '''
                if pid == 'FE':
                    if crd == '7':
                        if did not in ('3593', '3406'):continue
                '''
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                if not dmeta_data:
                    dmeta_data = {}
                tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
            for cid, us_nm in cid_dct.iteritems():
                if cid not in (cmp_rid, ):continue
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                m_id_tup = pc_info_dct[pid][cid]
                m_id  = m_id_tup[1].get("model_id", pid)
                rc_id = m_id_tup[1].get("rc_id", pid)
                ul_id = cid #m_id_tup[2]
                p_dct['info'].append({'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'monitor_id':ul_id})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        p_dct['info'][-1][k]    =v
            res_lst.append(p_dct)
        #p_inf = self.read_hfs_info(m_cur, m_conn)
        #res_lst.append(p_inf)
        m_conn.close()
        return [{'message':'done', 'data':res_lst}]
        
    def only_doc_info(self, ijson):
        project_id        = ijson['project_id']
        company_row_id    = ijson['crid']
        user_name = ijson.get('user', 'demo')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages, user_name FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' AND company_row_id='%s' """%(project_id, company_row_id)
        m_cur.execute(dc_qry)
        dc_data = m_cur.fetchall()
        unikeys = {}
        dc_data = list(dc_data)
        dc_data.sort(key=lambda x:int(x[1]))
        res_lst = []
        for dr in dc_data:
            crd, did, dmeta_data, sts, no_of_pages, usr_nm = dr 
            '''
            if pid == 'FE':
                if crd == '7':
                    if did not in ('3593', '3406'):continue
            '''
            dmeta_data = eval(dmeta_data)
            if not no_of_pages:
                no_of_pages = ''
            if not dmeta_data:
                dmeta_data = {}
            tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages, 'user':usr_nm}
            for dk, dv in dmeta_data.iteritems():
                #if dv:''
                #    unikeys[dk] = dv
                tmpdd[dk]   = dv
            res_lst.append(tmpdd)
        return [{'message':'done', 'data':res_lst}]
    
    def doc_info_dynamic(self, ijson):
        project_id        = ijson['project_id']
        company_row_id    = ijson['crid']
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        read_qry   = """ SELECT doc_id, meta_data FROM document_mgmt WHERE project_id=%s AND company_row_id=%s """%(project_id, company_row_id)
        m_cur.execute(read_qry)
        t_data     = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            doc_id, meta_data = map(str, row)
            d = {'dco_id':doc_id}
            d.update(meta_data)
            res_lst.append(d)
        return [{'message':'done', 'data':res_lst}]

    def read_project_comapny_mgmt_key(self):
        num_key_dct = self.key_map() 

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        
        read_pc = """ SELECT project_id, company_row_id, user_name FROM project_company_mgmt; """
        m_cur.execute(read_pc)
        pc_data = m_cur.fetchall()
    
        pc_info_dct = {}         
        for row in pc_data:
            project_id, company_id, user_name = map(str, row)
            pc_info_dct.setdefault(project_id, {})[company_id] = user_name
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE project_id in (%s) """%(pid_str)
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        
 
        res_lst = []
        for pid, cid_dct in pc_info_dct.iteritems():
            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s)  """%(cid_str)
            m_cur.execute(cid_dct_qry)
            c_data = m_cur.fetchall()
            p_dct = {num_key_dct['project_id']: pid, num_key_dct['project_name']: p_name, num_key_dct['desc']: description, num_key_dct['info']:[], num_key_dct['db_name']:db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s'  """%(pid)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                tmpdd   = {num_key_dct['d']:did, num_key_dct['status']:sts, num_key_dct['nop']:no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
                    
            for cid, us_nm in cid_dct.iteritems():
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                p_dct[num_key_dct['info']].append({num_key_dct['company_name']:cd_name, num_key_dct['company_id']:c_name, num_key_dct['user']:uer_nme, num_key_dct['info']:get_dc_info, num_key_dct['crid']:cid})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        get_k = num_key_dct.get(k, self.get_max_key(num_key_dct))
                        num_key_dct[k] = get_k
                        p_dct['info'][-1][num_key_dct[k]]    = v

            res_lst.append(p_dct)
        m_conn.close()
        map_key_dct = {iv:ik for ik, iv in num_key_dct.iteritems()}
        return [{'message':'done', 'data':res_lst, 'map_key_dct':map_key_dct}]

    def key_map(self):
        num_key_dct = {
                    'project_id':1, 
                    'project_name':2, 
                    'desc':3,
                    'info':4,
                    'db_name':5,
                    'd':6,  
                    'status':7,
                    'nop':8,    
                    'company_name':9,
                    'company_id':10, 
                    'user':11,  
                    'crid':12,              
                        }
        return num_key_dct    

    def get_max_key(self, num_key_dct):
        get_max = max(num_key_dct.values())
        new_key = get_max + 1
        return new_key
    
    def insert_project_module_info(self, ijson):
        project_id     = ijson["project_id"]
        module_name    = ijson["module_name"]
        module_key     = ijson["module_key"]
        module_parent  = ijson["module_parent"]
        user_name      = ijson["user_name"]
        d_time         = str(datetime.datetime.now())
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)

        #crt_stmt_pm = """  CREATE TABLE IF NOT EXISTS project_module_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL , module_key VARCHAR(256) DEFAULT NULL,  module_name VARCHAR(256) DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL)   """

        read_qry = """ SELECT project_id, module_parent, row_id FROM project_module_mgmt; """
        m_cur.execute(read_qry)
        pm_data = m_cur.fetchall()
        pm_res  = {}
        get_max = 0
        for row in pm_data:
            project_id, m_p = map(str, row[:2])
            row_id = int(row_id)
            pm_res[(project_id, m_p)]  = row_id
            
        if (project_id, module_parent) not in pm_res:
            insert_stmt = """ INSERT INTO project_module_mgmt(project_id, module_parent, user_name, insert_time) VALUES('%s', '%s', '%s', '%s')  """%(project_id, module_parent, user_name, d_time)
            m_cur.execute(insert_stmt)
            m_conn.commit()
            read_last = """ SELECT row_id from project_module_mgmt ORDER BY row_id DESC LIMIT 1; """
            m_cur.execute(read_last)
            last_row = int(m_cur.fetchone()[0])
        else:
            last_row = pm_res[((project_id, m_p))]
            
        #crt_stmt_mm = """ CREATE TABLE IF NOT EXISTS module_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL , module_key VARCHAR(256) DEFAULT NULL,  module_name VARCHAR(256) DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL)
        
        read_qry = """ SELECT parent_row_id, module_key, order_id FROM module_mgmt  """%(last_row)
        m_cur.execute(read_qry)
        mm_data = m_cur.fetchall()
        
        mm_dct = {(str(row[0]), str(row[1])):str(row[2])   for row in mm_data}

        if (last_row, module_key) not in mm_dct:
            get_max_order_id = int(max(mm_dct.values()))+1
            ins_stmt = """ INSERT INTO module_mgmt(module_name, module_key, parent_row_id, order_id) """%(module_name, module_key, last_row, get_max_order_id)
            m_cur.execute(ins_stmt)
            m_conn.commit()
        m_conn.close()
        return 'done'

    def read_stage_list_info(self, ijson):
        project_id = ijson['project_id']
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'WorkSpaceDb_DB']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doctype = ijson['type']
        if doctype.lower() == 'html':
            read_qry = """ select pstage_id, pstage_order, UserID from project_hstage_mgmt where ProjectID='%s';  """%(project_id) 
        else:
            read_qry = """ select pstage_id, pstage_order, UserID from project_pstage_mgmt where ProjectID='%s';  """%(project_id) 
        #  pstage_id | pstage_order | UserID
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        if not t_data:
            if doctype.lower() == 'html':
                read_qry = """ select pstage_id from hstage_mgmt"""
            else:
                read_qry = """ select pstage_idD from pstage_mgmt"""
            m_cur.execute(read_qry)
            t_data = m_cur.fetchall()
            
        m_conn.close()
        res_lst = []
        for row in t_data:
            #dt = {'pstage_id':row[0], 'pstage_order':row[1], 'uid':row[2]} 
            res_lst.append(str(row[0]))
        return [{'message':'done', 'data':res_lst}]
        
    def status_update(self, ijson):
        project_id = str(ijson['project_id']) 
        #doc_ids    = ', '.join(map(str, ijson['doc_id']))
        doc_ids    = str(ijson['doc_id'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        update_stmt = """ UPDATE document_mgmt SET status='Y'  WHERE doc_id='%s' AND project_id='%s' """%(doc_ids, project_id)
        m_cur.execute(update_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]        
