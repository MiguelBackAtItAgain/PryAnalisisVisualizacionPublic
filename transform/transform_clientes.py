from transform.transfomations import *

import pandas as pd
import traceback

def tra_client(etl_id, ses_db_stg):
    try:
        client_ext_transf_dic = {
            "id_cli": [],
            "identificacion_cli" : [],
            "nombre_cli": [],
            "tipo_cli": [],
            "sucursal_cli" : [],
            "aps_cli": [],
            "id_plz_cli": [],
            "id_cal_cli": [],
            "etl_id": []
        }

        client_ext = pd.read_sql('SELECT ID_CLI, IDENTIFICACION_CLI, NOMBRE_CLI, TIPO_CLI, SUCURSAL_CLI, APS_CLI, ID_PLZ_CLI ,ID_CAL_CLI FROM clientes_ext', ses_db_stg)

        if client_ext.empty:
            return
        
        for id, cedula, nombre, tipo, sucursal, aps, poliza, categoria \
        in zip(client_ext["ID_CLI"], client_ext["IDENTIFICACION_CLI"],
            client_ext["NOMBRE_CLI"], client_ext["TIPO_CLI"], client_ext["SUCURSAL_CLI"],
            client_ext["APS_CLI"], client_ext["ID_PLZ_CLI"], client_ext["ID_CAL_CLI"]):

            client_ext_transf_dic["id_cli"].append(str_2_int(id)),
            client_ext_transf_dic["identificacion_cli"].append(str_2_int(cedula)),
            client_ext_transf_dic["nombre_cli"].append(nombre),
            client_ext_transf_dic["tipo_cli"].append(obt_tipo_cliente(tipo)),
            client_ext_transf_dic["sucursal_cli"].append(sucursal),
            client_ext_transf_dic["aps_cli"].append(aps),
            client_ext_transf_dic["id_plz_cli"].append(str_2_int(poliza)),
            client_ext_transf_dic["id_cal_cli"].append(str_2_int(categoria))
            client_ext_transf_dic["etl_id"].append(etl_id)

        if client_ext_transf_dic["id_cli"]:
            df_client_tra = pd.DataFrame(client_ext_transf_dic)
            sanitized_names = df_client_tra["nombre_cli"].apply(lambda n: remove_titles(n))
            first_names = sanitized_names.apply(lambda n: n.split(' ')[0])
            last_names = sanitized_names.apply(lambda n: n.split(' ')[1])       
            df_client_tra['nombre_cli'] = first_names
            df_client_tra['apellido_cli'] = last_names
            df_client_tra.to_sql('clientes_tra', ses_db_stg, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass

def remove_titles(x):
    titles = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'DVM', 'MD', 'PhD', 'DDS']
    for title in titles:
        if title in x:
            x = x.replace(title, "")
            x = x.strip()
    return x