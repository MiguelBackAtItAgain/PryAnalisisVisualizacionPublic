from transform.tranform_productos import tra_productos
from transform.tranform_polizas import tra_polizas
from transform.transform_calificacion_sc import tra_calificacion
from transform.transform_motivos_cancelacion import tra_motivos
from transform.transform_clientes import tra_client

import traceback

def transform_all(etl_id, ses_db_stg):
    try:
        tra_productos(etl_id, ses_db_stg)
        tra_polizas(etl_id, ses_db_stg)
        tra_calificacion(etl_id, ses_db_stg)
        tra_motivos(etl_id, ses_db_stg)
        tra_client(etl_id, ses_db_stg)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass