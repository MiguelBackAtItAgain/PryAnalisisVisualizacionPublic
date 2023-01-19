import pandas as pd
import traceback

def ext_productos(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/productos.csv"
        producto_dic = {
            "id_pro" : [],
            "nombre_pro" : [],
            "estado_pro" : [],
        }

        producto_csv = pd.read_csv(path)
        #print(producto_csv)

        #Procesar los archivos csv

        if not producto_csv.empty:
            for id_pro, nom_pro, est_pro \
                in zip(producto_csv['ID_PRO'],
                        producto_csv['NOMBRE_PRO'],
                        producto_csv['ESTADO_PRO']): 

                        producto_dic["id_pro"].append(id_pro),
                        producto_dic["nombre_pro"].append(nom_pro),
                        producto_dic["estado_pro"].append(est_pro)
        if producto_dic["id_pro"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE productos_ext")
            df_productos_ext = pd.DataFrame(producto_dic)
            df_productos_ext.to_sql('productos_ext',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass