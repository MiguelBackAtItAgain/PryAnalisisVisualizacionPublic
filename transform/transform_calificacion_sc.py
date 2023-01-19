from transform.transfomations import *

import pandas as pd
import traceback

def tra_calificacion(etl_id, ses_db_stg):
    try:
        #Diccionario de los valores
        calificacion_tra_dic = {
            "id_cal" : [],
            "nombre_cal" : [],
            "etl_id" : [],
        }

        calificacion_ext = pd.read_sql("SELECT ID_CAL, NOMBRE_CAL FROM calificacion_sc_ext", ses_db_stg)

        if not calificacion_ext.empty:
            for id, cal \
                in zip(calificacion_ext['ID_CAL'],
                        calificacion_ext['NOMBRE_CAL']):
                     
                        calificacion_tra_dic["id_cal"].append(str_2_int(id)),
                        calificacion_tra_dic["nombre_cal"].append(cal),
                        calificacion_tra_dic["etl_id"].append(etl_id)
        if calificacion_tra_dic["id_cal"]:
            df_calificacion_tra = pd.DataFrame(calificacion_tra_dic)
            df_calificacion_tra.to_sql('calificacion_sc_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass