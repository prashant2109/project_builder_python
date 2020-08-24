import os, sys, xlsxwriter, copy, MySQLdb, ast
import ConfigParser
import common.xlsxReader as xlsxReader
from utils.meta_info import MetaInfo
from collections import OrderedDict as OD
#from meta_info import  Meta_info

class PYAPI(MetaInfo):
    
    def __init__(self, pypath="/root/industry/industry_classification/pysrc"):
        MetaInfo.__init__(self, pypath)
        self.objX           = xlsxReader.xlsxReader()  
        
    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
    
    def insert_uploaded_xl_meta(self, file_name, user_name, status):
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        crt_stmt = """CREATE TABLE IF NOT EXISTS excel_meta(row_id INT NOT NULL AUTO_INCREMENT, file_name VARCHAR(256), data_time DATETIME DEFAULT NULL, user_name VARCHAR(32), status VARCHAR(10), PRIMARY KEY (row_id))"""
        m_cur.execute(crt_stmt)
        insert_stmt = """INSERT INTO excel_meta(file_name, data_time, user_name, status) VALUES('%s', Now(), '%s', '%s')"""%(file_name, user_name, status)  
        m_cur.execute(insert_stmt)
        m_conn.commit()
        m_conn.close()
        return 'done'
        
    
    def jn_read_excel_data_info_tree(self, ijson):
        get_excel_id = str(ijson['excel_id'])
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        rd_qry =  """ SELECT edi.overlap, edi.segment, edi.flag, eni.master_group_name, eni.site_customer_name, edi.accept_reject_info, edi.company_id, edi.row_id  FROM excel_data_info as edi INNER JOIN excel_name_info as eni where edi.company_id = eni.row_id  """
        m_cur.execute(rd_qry)
        exd_data = m_cur.fetchall()
            
        cid_dct = OD()
        for row in exd_data:
            overlap, segment, flag, master_group_name, site_customer_name, accept_reject_info, company_id, row_id = row
            if accept_reject_info == 'R':continue
            cid_dct.setdefault((company_id, master_group_name, site_customer_name), []).append((overlap, segment, flag, row_id, accept_reject_info))
        
        #tmp_cid = 0
        resultant_data_list = []
        idx = 1
        for (cid, get_mgn, get_scn), data_lst in cid_dct.iteritems():
            get_len_rowspan = len(data_lst) 
            haschild = False
            if get_len_rowspan > 1:
                haschild = True
            
            res_data_dct = {'children':[], 'cnt':get_len_rowspan-1}
            for dx, tup in  enumerate(data_lst, 1):
                olp, seg, flg, row_id, ar_flg = tup
                dt_dct = {'m_g':'', 'c_n':'', 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':1, 'rid':row_id, 'cid':cid, 'group':'%s_%s'%(idx, dx-1), 'ar_flg':ar_flg}
                if dx == 1:
                    dt_dct = {'m_g':get_mgn, 'c_n':get_scn, 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':get_len_rowspan, 'rid':row_id, 'cid':cid, 'group':'%s'%(idx), 'hasChild':False, 'ar_flg':ar_flg}
                    if haschild:
                        dt_dct = {'m_g':get_mgn, 'c_n':get_scn, 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':get_len_rowspan, 'rid':row_id, 'cid':cid, 'hasChild':True, 'group':'%s'%(idx), 'ar_flg':ar_flg}
                    res_data_dct.update(dt_dct)
                else:   
                    res_data_dct['children'].append(dt_dct)
                
            resultant_data_list.append(res_data_dct)
            idx += 1
        #for k in resultant_data_list:
        #    print k
        #    print
        #sys.exit()
        return [{'message':'done', 'data':resultant_data_list}]
    
    def read_excel_data(self, csv_file, sheet_names=[], ret_flg=None):
        import xlrd
        xl_workbook = xlrd.open_workbook(csv_file)
        if not sheet_names:
            sheet_names = xl_workbook.sheet_names()
        f_d = {}
        f_cell_d = {}
        for i in range(0,len(sheet_names)):
            xl_sheet        = xl_workbook.sheet_by_name(sheet_names[i])
            num_rows    = xl_sheet.nrows
            num_cols    = xl_sheet.ncols
            herader     = []
            data        = []
            for row_idx in range(0, num_rows):
                tmp_d           = {}
                if row_idx ==0:
                    for col_idx in range(0, num_cols):
                        val = xl_sheet.cell(row_idx, col_idx).value
                        if isinstance(val, unicode):
                            val = val.encode('utf-8')
                        if isinstance(val, str):
                            val = ' '.join(val.strip().split())
                        herader.append(val)
                    continue
                #print '\n=================================='
                for col_idx in range(0, num_cols):
                    val = xl_sheet.cell(row_idx, col_idx).value
                    cell_name   = xlrd.formula.cellname(row_idx, col_idx)
                    f_cell_d.setdefault(' '.join(sheet_names[i].split()).strip(), {})[cell_name]    = (len(data), herader[col_idx])
                    if isinstance(val, unicode):
                        val = val.encode('utf-8')
                    if isinstance(val, str):
                        val = ' '.join(val.strip().split())
                    else:
                        val = str(val)
                    #print [herader[col_idx], val, xl_sheet.cell(row_idx, col_idx).value]
                    cmt     = ''
                    page    = ''
                    ph      = ''
                    tmp_d[herader[col_idx]] = val
                    if ret_flg == 'Y':
                        tmp_d[('RC', herader[col_idx])] = (row_idx, col_idx)
                data.append(tmp_d)
            f_d[' '.join(sheet_names[i].split()).strip()] = (herader, data)
        if ret_flg == 'Y':
            f_d['FINFO_TT'] = f_cell_d
        return f_d

    def read_xl_data_store_it(self, file_name, user_name, status):
        self.insert_uploaded_xl_meta(file_name, user_name, status)
        data     = self.read_excel_data(file_name, sheet_names=[], ret_flg=None)

        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT row_id FROM excel_meta WHERE file_name='%s' """%(file_name) 
        m_cur.execute(read_qry)
        get_excel_id = m_cur.fetchone()[0]
        
        res_lst = []
        map_dct = {}
        m_g_n, s_c = '', ''
        for sheet_name, data_tup in data.iteritems():
            headers = data_tup[0]
            data_lst = data_tup[1]
            for dct in data_lst:
                mgn, scn, overlap, segment, flag  = dct['Mastergroup Name'], dct['Site Customer Name'], dct['Overlap'], dct['Segment'], dct['Flag']
                if mgn and scn:
                    m_g_n = copy.deepcopy(mgn) 
                    s_c   = copy.deepcopy(scn)
                    map_dct.setdefault((m_g_n, s_c), []).append((overlap, segment, flag))
                    res_lst.append((get_excel_id, mgn, scn, sheet_name))
                if not mgn and not scn:
                    map_dct.setdefault((m_g_n, s_c), []).append((overlap, segment, flag))

        crt_qry = """CREATE TABLE IF NOT EXISTS excel_name_info(row_id INT NOT NULL AUTO_INCREMENT, excel_id VARCHAR(256), master_group_name VARCHAR(1024), site_customer_name VARCHAR(1024), sheet_name VARCHAR(1024), PRIMARY KEY (row_id))""" 
        m_cur.execute(crt_qry)

    
        m_cur.executemany("""INSERT INTO excel_name_info(excel_id, master_group_name, site_customer_name, sheet_name) VALUES(%s, %s, %s, %s) """, res_lst)
        m_conn.commit()
    
        
        rd_qry = """SELECT row_id, master_group_name, site_customer_name FROM excel_name_info WHERE excel_id='%s' """%(get_excel_id)
        m_cur.execute(rd_qry)
        i_data = m_cur.fetchall()
        
        row_map = {}
        for row in i_data:
            rid, master_gn, site_cn  = row
            row_map[(master_gn, site_cn)] = rid
        
        data_rows = []
        for dt_tup, info_lst in map_dct.iteritems():
            for info_tup in info_lst: 
                get_cid = row_map[(dt_tup)]
                d_tup = (get_cid, ) + info_tup
                data_rows.append(d_tup)
        
        ct_qry = """CREATE TABLE IF NOT EXISTS excel_data_info(row_id INT NOT NULL AUTO_INCREMENT, company_id VARCHAR(1024), overlap VARCHAR(1024), segment VARCHAR(256), flag VARCHAR(32), PRIMARY KEY (row_id))"""
        m_cur.execute(ct_qry)
        
        m_cur.executemany("""INSERT INTO excel_data_info(company_id, overlap, segment, flag) VALUES(%s, %s, %s, %s) """, data_rows)
        m_conn.commit()
        m_conn.close()
    
    def read_all_excel_ids(self):
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        read_qry = """SELECT row_id, file_name, user_name, data_time, status FROM excel_meta;"""
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            row_id, file_name, user_name, data_time, status = map(str, row)
            dct = {'excel_id':row_id, 'file_name':file_name.split('.')[0], 'user_name':user_name, 'date_time':data_time, 'status':status}
            res_lst.append(dct)
        return [{'message':'done', 'data':res_lst}]
    

    def delete_rows_from_read_data_info(self, ijson):
        get_rid = str(ijson['rid'])
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        del_stmt = """DELETE  from  excel_data_info WHERE row_id in (%s) """%(get_rid)
        m_cur.execute(del_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def accept_reject_info_update(self, ijson):
        get_rid = str(ijson['rid'])
        ar_flg  = str(ijson['ar_flg'])
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        del_stmt = """ UPDATE excel_data_info SET accept_reject_info='%s' WHERE row_id in (%s) """%(ar_flg ,get_rid)
        m_cur.execute(del_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def search_from_google_get_results(self, ijson):
        search_text_lst = map(lambda txt:txt.strip(), ijson['search_lst'])
        #search_text_lst = ['python']
        qelm = [('Entity', 'Company')]
        data =  {'Company':search_text_lst}
        render_flag = 0
        exists_query = {}

        import google_search as gs
        gsObj = gs.Search()
        query, entity, urls_lst = gsObj.search(qelm, data, render_flag, exists_query)
    
        res_lst = []
        for row_lst in  urls_lst:
            key, value, desc   = row_lst
            if isinstance(key, unicode):
                key = key.encode('utf-8')
            dt = {'key':key, 'link':value, 'desc':desc}
            res_lst.append(dt)
        return [{'message':'done', 'data':res_lst}]

    def read_excel_data_info_tree(self, ijson):
        get_excel_id = str(ijson['excel_id'])
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'industrial_classification']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        id_name_map = {}
        read_qry = """SELECT row_id, master_group_name, site_customer_name FROM excel_name_info WHERE excel_id='%s' """%(get_excel_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        for rw in t_data:
            row_id, master_group_name, site_customer_name = rw
            id_name_map[str(row_id)] = (master_group_name, site_customer_name)

        get_keys_cid = ', '.join(["'" +e+ "'" for e in id_name_map.keys()])
        
        rd_qry = """SELECT company_id, overlap, segment, flag, row_id, accept_reject_info FROM excel_data_info WHERE company_id in (%s) """%(get_keys_cid)
        m_cur.execute(rd_qry)
        exd_data = m_cur.fetchall()
            
        cid_dct = OD()
        for row in exd_data:
            company_id, overlap, segment, flag, rid, accept_reject_info = row
            if accept_reject_info == 'R':continue
            cid_dct.setdefault(company_id, []).append((overlap, segment, flag, rid, accept_reject_info))
        
        #tmp_cid = 0
        resultant_data_list = []
        idx = 1
        for cid, data_lst in cid_dct.iteritems():
            get_len_rowspan = len(data_lst) 
            haschild = False
            if get_len_rowspan > 1:
                haschild = True
            get_mgn, get_scn = id_name_map[cid]
            res_data_dct = {'children':[], 'cnt':get_len_rowspan-1}
            for dx, tup in  enumerate(data_lst, 1):
                olp, seg, flg, row_id, ar_flg = tup
                dt_dct = {'m_g':'', 'c_n':'', 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':1, 'rid':row_id, 'cid':cid, 'group':'%s_%s'%(idx, dx-1), 'ar_flg':ar_flg}
                if dx == 1:
                    dt_dct = {'m_g':get_mgn, 'c_n':get_scn, 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':get_len_rowspan, 'rid':row_id, 'cid':cid, 'group':'%s'%(idx), 'hasChild':False, 'ar_flg':ar_flg}
                    if haschild:
                        dt_dct = {'m_g':get_mgn, 'c_n':get_scn, 'olp':olp, 'seg':seg, 'flag':flg, 'sn':idx, 'rowspan':get_len_rowspan, 'rid':row_id, 'cid':cid, 'hasChild':True, 'group':'%s'%(idx), 'ar_flg':ar_flg}
                    res_data_dct.update(dt_dct)
                else:   
                    res_data_dct['children'].append(dt_dct)
                
            resultant_data_list.append(res_data_dct)
            idx += 1
        return [{'message':'done', 'data':resultant_data_list}]

    def node_tree_info_read(self, doc, con_flg, type_structure):
        if con_flg:
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/NGramAnalysis/Main_%s/%s_NodeMap.txt'%(doc, type_structure)
            f = open(path)
            txt_data = f.read()
            f.close()
            txt_obj = {}
            if txt_data:
                txt_obj = ast.literal_eval(txt_data)
        elif not con_flg:   
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/docs/%s/NGramAnalysis/Main/%s_NodeMap.txt'%(doc, type_structure)
            f = open(path)
            txt_data = f.read()
            f.close()
            if txt_data:    
                txt_data = ast.literal_eval(txt_data)
            else:
                txt_data = {}
            txt_obj = {}
            for node_id, info_lst in txt_data.iteritems():
                ch_lst = []
                for tup in info_lst:
                    start, end = tup[1]
                    custom_index = tup[2]
                    dt_dct = {'x':custom_index, 'c':'_'.join([str(start), str(end)]), 'pno':tup[0]}
                    ch_lst.append(dt_dct)
                txt_obj[node_id] = ch_lst
        return txt_obj
    
    def rec_tree_info(self, ijson):
        doc_ids = ijson['doc_id']
        #doc_ids = doc_ids + ['_'.join(map(str, doc_ids))]
        dc_str = '_'.join(map(str, doc_ids))
        doc_ids = doc_ids + [dc_str]
        res_dct = {'consolidated':[]}
        for idx, doc in  enumerate(doc_ids, 1):
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/docs/%s/NGramAnalysis/Main/Lexical_Tree.txt'%(doc)
            con_flg = 0
            if idx == len(doc_ids):
                con_flg = 1
                path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/NGramAnalysis/Main_%s/Lexical_Tree.txt'%(doc)
            node_info_dct = self.node_tree_info_read(doc, con_flg)
            f  = open(path)
            txt_data = f.readlines()
            f.close()
            check_dct = OD()
            for ln in txt_data:
                m_id, m_info = ln.strip().split('@')
                get_node_info = node_info_dct.get(m_id, {})
                check_dct[m_id] = [0, m_info, len(m_id.split('.')) - 1, get_node_info]
                mid_lst = m_id.split('.')
                if len(mid_lst) > 1:
                    ch_dt_str = '.'.join(mid_lst[:-1])
                    check_dct[ch_dt_str][0] += 1  

            resultant_lst = []
            for m_id, val_tup in check_dct.iteritems():
                hasChild = False
                if val_tup[0]:
                    hasChild = True
                dt_dct = {'id':m_id, 'info':val_tup[1], 'level_id':val_tup[2], '$$treeLevel':val_tup[2], 'hasChild':hasChild, 'node_info':val_tup[3]}            
                resultant_lst.append(dt_dct)
            if con_flg:
                res_dct['consolidated'] = resultant_lst
            else:
                res_dct[str(doc)] = resultant_lst
        
        return [{'message':'done', 'data':res_dct}]

            
    def rec_tree_info_nhas(self, ijson):
        doc_ids = ijson['doc_id']
        doc_ids = doc_ids + ['_'.join(map(str, doc_ids))]
        
        res_dct = {'consolidated':[]}
        for idx, doc in  enumerate(doc_ids, 1):
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/docs/%s/NGramAnalysis/Main/FinalTree.txt'%(doc)
            con_flg = 0
            if idx == len(doc_ids):
                con_flg = 1
                path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/NGramAnalysis/Main_%s/FinalTree.txt'%(doc)
            f  = open(path)
            txt_data = f.readlines()
            f.close()
            resultant_lst = []
            for ln in txt_data:
                m_id, m_info = ln.strip().split('@')
                m_level = len(m_id.split('.')) - 1
                dt_dct = {'id':m_id, 'info':m_info, 'level':m_level}
                resultant_lst.append(dt_dct)
            if con_flg:
                res_dct['consolidated'] = resultant_lst
            else:
                res_dct[str(doc)] = resultant_lst
        return [{'message':'done', 'data':res_dct}]
        
    def rec_tree_info_consolidated_individual(self, doc_ids, ci_flg, type_structure):
        dc_str = '_'.join(map(str, doc_ids))
        path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/docs/%s/NGramAnalysis/Main/%s_Tree.txt'%(dc_str, type_structure)
        if ci_flg:
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/NGramAnalysis/Main_%s/%s_Tree.txt'%(dc_str, type_structure)
        node_info_dct = self.node_tree_info_read(dc_str, ci_flg, type_structure)
        f  = open(path)
        txt_data = f.readlines()
        f.close()
        check_dct = OD()
        for ln in txt_data:
            m_id, m_info = ln.strip().split('@')
            get_node_info = node_info_dct.get(m_id, {})
            check_dct[m_id] = [0, m_info, len(m_id.split('.')) - 1, get_node_info]
            mid_lst = m_id.split('.')
            if len(mid_lst) > 1:
                ch_dt_str = '.'.join(mid_lst[:-1])
                check_dct[ch_dt_str][0] += 1  
        #for k, v in check_dct.iteritems():
        #    print k, v, '\n'

        resultant_lst = []
        for m_id, val_tup in check_dct.iteritems():
            hasChild = False
            if val_tup[0]:
                hasChild = True
            get_match = val_tup[3]
            if ci_flg:
                dt_dct = {'id':m_id, 'info':val_tup[1], 'level_id':val_tup[2], '$$treeLevel':val_tup[2], 'hasChild':hasChild, 'match':get_match} 
                if not get_match:
                    dt_dct = {'id':m_id, 'info':val_tup[1], 'level_id':val_tup[2], '$$treeLevel':val_tup[2], 'hasChild':hasChild} 
            elif not ci_flg:
                dt_dct = {'id':m_id, 'info':val_tup[1], 'level_id':val_tup[2], '$$treeLevel':val_tup[2], 'hasChild':hasChild, 'node_info':get_match} 
            print dt_dct,'\n'
            resultant_lst.append(dt_dct)
        return [{'message':'done', 'data':resultant_lst, 'l':len(resultant_lst)}]
    
    def consolidated_info_tree(self, ijson):
        doc_ids = ijson['doc_id']
        type_structure  = ijson['type']
        res = self.rec_tree_info_consolidated_individual(doc_ids, 1, type_structure) 
        return res

    def individual_info_tree(self, ijson):
        doc_ids = ijson['doc_id']
        type_structure  = ijson['type']
        res = self.rec_tree_info_consolidated_individual(doc_ids, 0, type_structure) 
        return res
    
    def get_node_information(self, ijson):
        match =  ijson['match']
        type_structure = ijson['type']
        
        def parent_node_info(node_d, chk_dct, res_lst = []):
            node_atr_splt = node_d.split('.')
             
            
        
        for doc, info_lst in match.iteritems():
            path = '/var/www/html/WorkSpaceBuilder_DB/39/1/pdata/docs/%s/NGramAnalysis/Main/%s_Tree.txt'%(doc, type_structure)
            node_info_dct = self.node_tree_info_read(doc, 0, type_structure)
            f  = open(path)
            txt_data = f.readlines()
            f.close()
            check_dct = OD()
            for ln in txt_data:
                m_id, m_info = ln.strip().split('@')
                get_node_info = node_info_dct.get(m_id, {})
                check_dct[m_id] = [0, m_info, len(m_id.split('.')) - 1, get_node_info]
                mid_lst = m_id.split('.')
                if len(mid_lst) > 1:
                    ch_dt_str = '.'.join(mid_lst[:-1])
                    check_dct[ch_dt_str][0] += 1  
            node_id = info_lst[0]

    def read_project_info(self):
        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'WorkSpaceDb_DB']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        read_qry = """ SELECT ProjectID, ProjectName, ProjectCode FROM  ProjectMaster WHERE ProjectStatus='Y'  """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            ProjectID, ProjectName, ProjectCode = row
            data_dct = {'project_id':ProjectID, 'project_name':ProjectName, 'db_name':ProjectCode} 
            res_lst.append(data_dct)
        return [{'message':'done', 'data':res_lst}]
    
    def project_wise_company_info(self, ijson):
        project_id = ijson['project_id']
        db_name    = ijson['db_name']    

        db_data_lst = ['172.16.20.10', 'root', 'tas123', '%s'%(db_name)]
        m_conn, m_cur = self.mysql_connection(db_data_lst)     
        
        read_qry = """ SELECT meta_data FROM batch_mgmt_upload """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
       
        comp_set = set()
        for row in t_data:
            try:
                meta_data = eval(row[0])
            except:meta_data = {}
            get_comp = meta_data.get('Company', '')
            if not get_comp:continue
            comp_set.add(get_comp)
        res_lst = list(comp_set)
        res_lst.sort()
        res = [{'message':'done', 'data':res_lst}]
        return res
    
    def company_wise_doc_info(self, ijson):
        project_id      = ijson['project_id']
        db_name         = ijson['db_name']    
        company_name    = ijson['company_name']

        db_data_lst = ['172.16.20.10', 'root', 'tas123', '%s'%(db_name)]
        m_conn, m_cur = self.mysql_connection(db_data_lst)     
        
        read_qry = """ SELECT meta_data, doc_id, doc_name, doc_type FROM batch_mgmt_upload """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        res_lst = []
        for row in t_data:
            doc_id, doc_name, doc_type = row[1:]
            meta_data = eval(row[0])
            get_comp = meta_data['Company']
            if get_comp != company_name:continue
            data_dct = {'d':doc_id, 'dn':doc_name, 'dt':doc_type, 'meta':meta_data}
            res_lst.append(data_dct)
        res_lst.sort(key=lambda x:int(x['d']))
        res = [{'message':'done', 'data':res_lst}]
        return res

if __name__ == '__main__':
    pObj = PYAPI()
    #pObj.rec_tree_info()
    #######################################################################################
    #pObj.read_xl_data_store_it('Horis_vs_QAD_overlap.xls', 'prashant', 'N') # insert_xl_data
    #pObj.read_xl_data_store_it('CAM_vs_QAD_overlap.xls', 'prashant', 'N')   # insert_xl_data
    #print pObj.read_all_excel_ids()
    #ijson = {"excel_id":'1'}
    

