import pandas as pd
import traceback

def ext_motivos_cancelacion(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/motivos_cancelacion.csv"
        motivo_dic = {
            "id_mot" : [],
            "descripcion_mot" : [],
            "estado_mot" : [],
        }

        motivo_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not motivo_csv.empty:
            for id, des, est\
                in zip(motivo_csv['ID_MOT'],
                        motivo_csv['DESCRIPCION_MOT'],
                        motivo_csv['ESTADO_MOT']):

                        motivo_dic["id_mot"].append(id),
                        motivo_dic["descripcion_mot"].append(des),
                        motivo_dic["estado_mot"].append(est),
        if motivo_dic["id_mot"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE motivos_cancelacion_ext")
            df_motivos_cancelacion_ext = pd.DataFrame(motivo_dic)
            df_motivos_cancelacion_ext.to_sql('motivos_cancelacion_ext',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass