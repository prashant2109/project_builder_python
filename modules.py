import sys, os, json, datetime, MySQLdb
import db.get_conn as get_conn
conn_obj = get_conn.DB()
import config

class Modules():
    def __init__(self):
        self.db_path = config.Config.s_dbinfo 

    def get_modules(self):
        conn, cur   = conn_obj.MySQLdb_connection(self.db_path)
        sql = "select row_id, name, m_key, full_name, fa_icon, doc_view, active from module_mgmt"
        cur.execute(sql)
        res = cur.fetchall()
        m_list = []
        for r in res:
            r_id, n, k, f_n, fi, d_v, s = r
            dic = {'r_id': r_id, 'n': n, 'k': k, 'f_n': f_n, 'fi': fi, 'd_v': d_v, 's': s}
            m_list.append(dic)
        return [{'message': 'done', 'data': m_list}]
    
    def save_modules(self, ijson):
        user = ijson['user']
        s_data = ijson['data']
        c_update = ijson['update']
        n = s_data['n']
        k = s_data['k']
        f_n = s_data.get('f_n', n)
        fi = s_data.get('fi', '')
        d_v = s_data.get('d_v', 'N')
        s = s_data.get('s', 'Y')
        up_time = datetime.datetime.now()
        conn, cur = conn_obj.MySQLdb_connection(self.db_path)
        if c_update=='Y':
            sql = "update module_mgmt set name='%s', full_name='%s', fa_icon='%s', doc_view='%s', active='%s', update_user='%s', update_time='%s' where m_key='%s'"%(n, f_n, fi, d_v, s, user, up_time, k)
            cur.execute(sql)
        else:
            cur.execute('INSERT INTO module_mgmt(name, m_key, full_name, fa_icon, doc_view, active, insert_user, update_time) VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")'%(n, k, f_n, fi, d_v, 'Y', user, up_time))
        conn.commit()
        conn.close()
        res = [{'message': 'done'}]
        return res

    def rpurpose_modules(self):
        m_conn, m_cur = conn_obj.MySQLdb_connection(self.db_path)
        read_qry = """ SELECT row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id FROM module_mgmt WHERE alive_status='Y' and parent_key !=0 """ 
        m_cur.execute(read_qry)
        
        t_data= m_cur.fetchall()
        m_conn.close()

        sort_info_cdict = {}
        data = {}
        map_dct = {}
        row_id_map = {}
        child_parent = {}
        for row in t_data:  
            row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id = map(str, row)
            if parent_key == '-1':
                parent_key = 'root'
            row_id_map[row_id] = m_key
            data[m_key]  = {'fa_icon':fa_icon, 'full_name':full_name, 'k':m_key, 'n':name, 'parent_key':parent_key, 'order_id':order_id, 'rid':row_id}
            map_dct.setdefault(parent_key, []).append(row_id) 
            sort_info_cdict[m_key] = int(order_id)
        
        
        c_dict = {}
        p_dict = {}
        r_data = {}
        for pkey, dt_lst in map_dct.iteritems():
            if pkey == 'root':
                gt_k = pkey 
            else:
                gt_k = row_id_map.get(pkey)
                if not gt_k:continue
            c_lst = []  
            for rds in dt_lst:
                gt_vl = row_id_map[rds]
                get_data_dct = data[gt_vl]
                r_data[gt_vl] = get_data_dct
                c_lst.append(gt_vl)
                p_dict[gt_vl] = gt_k
            c_lst.sort(key=lambda x:sort_info_cdict[x])
            c_dict[gt_k] = c_lst
        return [{'message':'done', 'data': r_data, 'p_dict': p_dict, 'c_dict': c_dict}]
        
    def read_updated_modules(self):
        m_conn, m_cur = conn_obj.MySQLdb_connection(self.db_path)
        read_qry = """ SELECT row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id FROM module_mgmt WHERE alive_status='Y' """ 
        m_cur.execute(read_qry)
        t_data= m_cur.fetchall()
        m_conn.close()

        sort_info_cdict = {}
        data = {}
        map_dct = {}
        p_dict = {}
        row_id_map = {}
        for row in t_data:  
            row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id = map(str, row)
            if doc_view == 'Y':
                doc_view = 1
            elif doc_view == 'N':
                doc_view = 0
            if parent_key == '-1':
                parent_key = 'root'
            row_id_map[row_id] = m_key
            data[m_key]  = {'rid':row_id, 'fa_icon':fa_icon, 'full_name':full_name, 'doc_view':doc_view, 'k':m_key, 'n':name, 'pk':parent_key, 'order_id':order_id, 'active':active,'submenu':[]}
            map_dct.setdefault(parent_key, []).append(row_id) 
            sort_info_cdict[m_key] = int(order_id)
        
        print map_dct
        def rec_func(c_ids, cl_id):    
            cl_id += 1
            cres_lst = []
            for c_rid in c_ids: 
                cmky = row_id_map[c_rid]
                cinf_data_dct = data[cmky]
                cinf_data_dct['level_id'] = cl_id
                gt_ky_lst = map_dct.get(c_rid, [])
                #if not gt_ky_lst:continue
                r_ls = []
                if gt_ky_lst:
                    r_ls = rec_func(gt_ky_lst, cl_id)
                cinf_data_dct['submenu'] = r_ls
                cres_lst.append(cinf_data_dct)
            cres_lst.sort(key=lambda x:sort_info_cdict[x['k']])
            return cres_lst

        res_lst = []
        get_all_roots = map_dct['root']
        for r_prnt_id in get_all_roots:
            l_id = 0
            pmky = row_id_map[r_prnt_id]
            pdata = data[pmky]
            pdata['level_id'] = 0
            chld_ids = map_dct.get(r_prnt_id, [])
            #if not chld_ids:continue
            cd_lst = []
            if chld_ids:
                cd_lst = rec_func(chld_ids, l_id)
            pdata['submenu'] = cd_lst
            res_lst.append(pdata)
        res_lst.sort(key=lambda x:sort_info_cdict[x['k']])
        return [{'message':'done', 'data':res_lst}]

    def update_modified_module_mgmt(self, ijson):
        update_data = ijson['data']
        update_rows = []
        check_rows  = []
        for row in update_data:
            row_id, parent_key, order_id = row['id'], row['p_id'], row['index'] 
            update_rows.append((parent_key, order_id, row_id))
            check_rows.append(row_id)
           
        print update_rows 
        m_key_str = ', '.join({'"'+str(e)+'"' for e in check_rows})    
        # (name, m_key, full_name, fa_icon, doc_view, active, insert_user, update_user)
        m_conn, m_cur = conn_obj.MySQLdb_connection(self.db_path)
        u_stmt = """ UPDATE module_mgmt SET parent_key=0, order_id=0, alive_status='N'  """
        m_cur.execute(u_stmt)
        update_stmt = """ UPDATE module_mgmt SET parent_key=%s, order_id=%s, alive_status='Y'  WHERE row_id=%s """
        m_cur.executemany(update_stmt, update_rows)
        #y_update = """ UPDATE modified_module_mgmt SET alive_status='N' WHERE row_id not in (%s) """%(m_key_str)
        #m_cur.execute(y_update) 
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
        
