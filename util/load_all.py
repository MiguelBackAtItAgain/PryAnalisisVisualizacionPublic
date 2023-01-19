from load.load_productos import load_productos
from load.load_motivos_cancelacion import load_motivos_cancelacion
from load.load_calificacion_sc import load_calificacion
from load.load_polizas import load_polizas
from load.load_clientes import load_clients



import traceback

def load_all(etl_id, ses_db_stg, ses_db_sor):
    try:
        load_productos(etl_id, ses_db_stg, ses_db_sor)
        load_motivos_cancelacion(etl_id, ses_db_stg, ses_db_sor)
        load_polizas(etl_id, ses_db_stg, ses_db_sor)
        load_calificacion(etl_id, ses_db_stg, ses_db_sor)
        load_clients(etl_id, ses_db_stg, ses_db_sor)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass