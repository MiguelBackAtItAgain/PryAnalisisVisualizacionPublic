from transform.transfomations import *

import pandas as pd
import traceback

def tra_polizas(etl_id, ses_db_stg):
    try:
        #Diccionario de los valores
        poliza_tra_dic = {
            "id_plz" : [],
            "estado_plz" : [],
            "id_pro_plz" : [],
            "inicio_vigencia_plz" : [],
            "fin_vigencia_plz" : [],
            "fecha_can_plz" : [],
            "valor_seguro_plz" : [],
            "id_mot_plz" : [],
            "etl_id" : [],
        }

        poliza_ext = pd.read_sql("SELECT ID_PLZ, ESTADO_PLZ, ID_PRO_PLZ, INICIO_VIGENCIA_PLZ, FIN_VIGENCIA_PLZ, FECHA_CAN_PLZ, VALOR_SEGURO_PLZ, ID_MOT_PLZ FROM polizas_ext", ses_db_stg)

        if not poliza_ext.empty:
            for id_plz, est_plz, id_pro_plz, ini_vig_plz, fin_vig_plz, fec_can_plz, val_seg_plz, id_mot_plz \
                in zip(poliza_ext['ID_PLZ'],
                        poliza_ext['ESTADO_PLZ'],
                        poliza_ext['ID_PRO_PLZ'],
                        poliza_ext['INICIO_VIGENCIA_PLZ'],
                        poliza_ext['FIN_VIGENCIA_PLZ'],
                        poliza_ext['FECHA_CAN_PLZ'],
                        poliza_ext['VALOR_SEGURO_PLZ'],
                        poliza_ext['ID_MOT_PLZ']): 

                        poliza_tra_dic["id_plz"].append(str_2_int(id_plz)),
                        poliza_tra_dic["estado_plz"].append(obt_estado_poliza(est_plz)),
                        poliza_tra_dic["id_pro_plz"].append(str_2_int(id_pro_plz)),
                        poliza_tra_dic["inicio_vigencia_plz"].append(obt_date_2(ini_vig_plz)),
                        poliza_tra_dic["fin_vigencia_plz"].append(obt_date_2(fin_vig_plz)),
                        poliza_tra_dic["fecha_can_plz"].append(obt_date_2(fec_can_plz)),
                        poliza_tra_dic["valor_seguro_plz"].append(str_2_float(val_seg_plz)),
                        poliza_tra_dic["id_mot_plz"].append(str_2_int(id_mot_plz)),
                        poliza_tra_dic["etl_id"].append(etl_id)
        if poliza_tra_dic["id_plz"]:
            df_poliza_tra = pd.DataFrame(poliza_tra_dic)
            df_poliza_tra.to_sql('polizas_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass