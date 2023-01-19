
from transform.transfomations import *

import pandas as pd
import traceback

def tra_motivos(etl_id, ses_db_stg):
    try:
        #Diccionario de los valores
        motivo_tra_dic = {
            "id_mot" : [],
            "descripcion_mot" : [],
            "estado_mot" : [],
            "etl_id" : [],
        }

        motivo_ext = pd.read_sql("SELECT ID_MOT, DESCRIPCION_MOT, ESTADO_MOT FROM motivos_cancelacion_ext", ses_db_stg)

        if not motivo_ext.empty:
            for id, des, est \
                in zip(motivo_ext['ID_MOT'],
                        motivo_ext['DESCRIPCION_MOT'],
                        motivo_ext['ESTADO_MOT']):
                 

                        motivo_tra_dic["id_mot"].append(str_2_int(id)),
                        motivo_tra_dic["descripcion_mot"].append(des),
                        motivo_tra_dic["estado_mot"].append(obt_estado_producto(est)),
                        motivo_tra_dic["etl_id"].append(etl_id)
        if motivo_tra_dic["id_mot"]:
            df_motivo_tra = pd.DataFrame(motivo_tra_dic)
            df_motivo_tra.to_sql('motivos_cancelacion_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass