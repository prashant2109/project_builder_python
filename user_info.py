import common.dbcrypt_intf as dbcrypt
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import datetime
class Login():
    def __init__(self, path):
        self.db_path = path

    def validate_login(self, ijson):
        '''istr    = ijson['istr']
        user_name, password = istr.split(':$:')
        vret = "incorrect"
        conn, cur = conn_obj.MySQLdb_connection(self.db_path)
        stmt = "select row_id, user_id, user_passwd, user_name, user_role, login_status from user_mgmt where user_id='%s'" %(user_name)
        cur.execute(stmt)
        rows = cur.fetchall()
        for row in rows:
            uid, user_id, tpassword, user_name, user_role, login_status = row
            if tpassword == password:
                vret = {'uid': uid, 'user_id': user_id, 'user_name': user_name, 'user_role':user_role, 'user_passwd':tpassword, 'login_status': login_status} #row[2] + '#' + row[3]
        if vret != "incorrect" and vret['login_status'] == 'Y' and vret['uid'] != 105 and 0:
            cur.close()
            conn.close()
            res = [{'message':'Already Logged in.'}]
            return res
        cur.close()
        conn.close()
        if vret == 'incorrect':
            return [{"message": "Incorrect Login Credentials."}]
        vret['message'] = 'done'
        res = [vret]
        return res'''
        
        user_id, user_pass  = ijson['istr'].split(':$:')
        conn, cur   = conn_obj.MySQLdb_connection(self.db_path)
        enc_user_pass   = dbcrypt.encryptIVfix(user_pass)
        sel_sql         = "select * from login_master where user_id = '%s' and user_passwd = '%s'"%(user_id, enc_user_pass)
        cur.execute(sel_sql)
        user_ar         = cur.fetchone()
        tdict = {}
        cur.close()
        conn.close()
        if user_ar:
            user_id,user_passwd,user_name,user_role,login_status,unique_key,user_time = user_ar
            tdict = {'user_id':user_id,'user_passwd':user_pass,'user_name':user_name,'user_role':user_role,'login_status':login_status,'unique_key':unique_key,'user_time':user_time}
            tdict['message'] = 'done'
            return [tdict]
        else:
            return [{"message": "Incorrect Login Credentials."}]
    
    def save_user_details(self, ijson):
        user_id = ijson['user_id']
        user_name = ijson.get('user_name', ijson['user_id'])
        user_pswrd = ijson.get('user_pswrd')
        user_role = ijson.get('user_role', 'user')
        enc = dbcrypt.encryptIVfix(user_pswrd)
        print user_id, user_name, user_pswrd, user_role, enc
        conn, cur = conn_obj.MySQLdb_connection(self.db_path)
        sql_stmt_2 = """SELECT user_id FROM login_master"""
        cur.execute(sql_stmt_2)
        res = cur.fetchall()
        for each in res:
           if user_id in each:
                return [{"message": "Login User Already Present..."}]
        sql_stmt = """INSERT
                      INTO login_master
                      ( user_id ,user_passwd, user_name, user_role,
                        login_status, unique_key, user_time )
                      VALUES('%s', '%s', '%s','%s', '%s', %s, '%s')
                      """%(user_id, enc, user_name, user_role, 'N', 0, '0')
        cur.execute(sql_stmt)
        conn.commit()
        cur.close()
        conn.close()
        return [{"message": "done", "text": "User is created"}]

if __name__  ==  '__main__':
    l_Obj = Login()

