from transform.transfomations import *

import pandas as pd
import traceback

def tra_productos(etl_id, ses_db_stg):
    try:
        #Diccionario de los valores
        producto_tra_dic = {
            "id_pro" : [],
            "nombre_pro" : [],
            "estado_pro" : [],
            "etl_id" : [],
        }

        producto_ext = pd.read_sql("SELECT ID_PRO, NOMBRE_PRO, ESTADO_PRO FROM productos_ext", ses_db_stg)

        if not producto_ext.empty:
            for id_pro, nom_pro, est_pro \
                in zip(producto_ext['ID_PRO'],
                        producto_ext['NOMBRE_PRO'],
                        producto_ext['ESTADO_PRO']): 

                        producto_tra_dic["id_pro"].append(str_2_int(id_pro)),
                        producto_tra_dic["nombre_pro"].append(nom_pro),
                        producto_tra_dic["estado_pro"].append(obt_estado_producto(est_pro)),
                        producto_tra_dic["etl_id"].append(etl_id)
        if producto_tra_dic["id_pro"]:
            df_producto_tra = pd.DataFrame(producto_tra_dic)
            df_producto_tra.to_sql('productos_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass