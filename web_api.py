import json, sys, os
from pyapi import PYAPI
import modules
m_obj = modules.Modules()
import db.get_conn as get_conn
conn_obj = get_conn.DB()
def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__
class WebAPI(PYAPI):
    def __init__(self):
        PYAPI.__init__(self)

    def process(self, cmd_id, ijson):
        res     = []
        if 1 == cmd_id:
            res = self.get_url_stats(ijson)
        elif 2 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.rm_user_filter_read_project_comapny_mgmt(ijson)
            #res = pObj.read_project_comapny_mgmt(ijson)
        elif 3 == cmd_id:
            import update_project_company_mgmt_pyapi as pyf
            pObj = pyf.PYAPI()
            res = pObj.update_insert_uploaded_doc_info(ijson)
        elif 4 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.only_doc_info(ijson)
        elif 5 == cmd_id:
            res = self.execute_url(ijson)
        elif 6 == cmd_id:
            res = self.setup_new_url(ijson)
        elif 7 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.read_all_modules(ijson)
        elif 8 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.user_save(ijson)
        elif 9 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.read_saved_info(ijson)
        elif 10 == cmd_id:
            res = self.upload_document_info(ijson)
        elif 11 == cmd_id:
            res = self.read_all_company_info(ijson)
        elif 12 == cmd_id:
            res = self.project_configuration(ijson) 
        elif 13 == cmd_id:
            res = self.scheduler_process_mgmt_insert(ijson) 
        elif 14 == cmd_id:
            res = self.insert_url_name_data(ijson)
        elif 15 == cmd_id:
            res = self.delete_url_data(ijson) 
        elif 16 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.read_stage_list_info(ijson) 
        elif 17 == cmd_id:
            import status_update as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.status_update(ijson) 
        elif 18 == cmd_id:
            res = self.get_schedule_info(ijson) 
        elif 19 == cmd_id:
            res = self.update_meta_10(ijson)
        elif 20 == cmd_id:
            res = self.validate_login(ijson)
        elif 21 == cmd_id:
            res = self.project_wise_doc_info_10(ijson)
        elif 22 == cmd_id:
            res = self.data_path_method_url_execution(ijson)
        elif 23 == cmd_id:
            res = self.add_new_company_docs(ijson)
        elif 24 == cmd_id:
            res = self.remove_company_docs(ijson)
        ###For Modules
        elif 25 == cmd_id:
            res = m_obj.get_modules()
        elif 26 == cmd_id:
            res = m_obj.save_modules(ijson)
        elif 27 == cmd_id:
            res = self.send_user_lst()
        elif 28 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            #res = pObj.rm_user_filter_read_project_comapny_mgmt(ijson)
            res = pObj.read_project_comapny_mgmt(ijson)
        elif 29 == cmd_id:
            res = self.save_user_configurations(ijson)
        elif 30 == cmd_id:
            res = self.user_wise_cofigured_project_data(ijson)
        elif 31 == cmd_id:
            res = m_obj.update_modified_module_mgmt(ijson)
        elif 32 == cmd_id:
            res = m_obj.rpurpose_modules()
        elif 33 == cmd_id:
            res = m_obj.read_updated_modules()
        elif 34 == cmd_id: # pre_taxo
            import pre_taxo_pyapi as pyf
            pObj = pyf.pyf()
            res = pObj.read_all_excel_ids(ijson)
        elif 35 == cmd_id:
            res = self.get_uml_data(ijson)
        return json.dumps(res)

if __name__ == '__main__':
    obj = WebAPI()
    try:
        ijson   = json.loads(sys.argv[1])
        cmd_id  = int(ijson['cmd_id'])
    except:
        cmd_id  = int(sys.argv[1])
        ijson   = {}
        if len(sys.argv) > 2:
            tmpjson = json.loads(sys.argv[2])
            ijson.update(tmpjson)
    if ijson.get('PRINT') != 'Y':
        disableprint()
    print ijson
    res = obj.process(cmd_id, ijson)
    enableprint()
    print res
