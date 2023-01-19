import pandas as pd
import traceback

def ext_polizas(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/polizas1.csv"
        poliza_dic = {
            "id_plz" : [],
            "estado_plz" : [],
            "id_pro_plz" : [],
            "inicio_vigencia_plz" : [],
            "fin_vigencia_plz" : [],
            "fecha_can_plz" : [],
            "valor_seguro_plz" : [],
            "id_mot_plz" : []
        }

        poliza_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not poliza_csv.empty:
            for id_plz, est_plz, id_pro_plz, ini_vig_plz, fin_vig_plz, fec_can_plz, val_seg_plz, id_mot_plz \
                in zip(poliza_csv['ID_PLZ'],
                        poliza_csv['ESTADO_PLZ'],
                        poliza_csv['ID_PRO_PLZ'],
                        poliza_csv['INICIO_VIGENCIA_PLZ'],
                        poliza_csv['FIN_VIGENCIA_PLZ'],
                        poliza_csv['FECHA_CAN_PLZ'],
                        poliza_csv['VALOR_SEGURO_PLZ'],
                        poliza_csv['ID_MOT_PLZ']): 

                        poliza_dic["id_plz"].append(id_plz),
                        poliza_dic["estado_plz"].append(est_plz),
                        poliza_dic["id_pro_plz"].append(id_pro_plz),
                        poliza_dic["inicio_vigencia_plz"].append(ini_vig_plz),
                        poliza_dic["fin_vigencia_plz"].append(fin_vig_plz),
                        poliza_dic["fecha_can_plz"].append(fec_can_plz),
                        poliza_dic["valor_seguro_plz"].append(val_seg_plz),
                        poliza_dic["id_mot_plz"].append(id_mot_plz)
        if poliza_dic["id_plz"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE polizas_ext")
            df_polizas_ext = pd.DataFrame(poliza_dic)
            df_polizas_ext.to_sql('polizas_ext',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass