import pandas as pd
import traceback

def ext_calificacion_sc(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/calificacion_sc.csv"
        calificacion_dic = {
            "id_cal" : [],
            "nombre_cal" : [],
        }

        calificacion_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not calificacion_csv.empty:
            for id, cal \
                in zip(calificacion_csv['ID_CAL'],
                        calificacion_csv['NOMBRE_CAL']):

                        calificacion_dic["id_cal"].append(id),
                        calificacion_dic["nombre_cal"].append(cal)
        if calificacion_dic["id_cal"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE calificacion_sc_ext")
            df_calificacion_sc_ext = pd.DataFrame(calificacion_dic)
            df_calificacion_sc_ext.to_sql('calificacion_sc_ext',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass