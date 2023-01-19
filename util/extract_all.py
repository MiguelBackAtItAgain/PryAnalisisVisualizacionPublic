from extract.extract_productos import ext_productos
from extract.extract_motivos_cancelacion import ext_motivos_cancelacion
from extract.extract_calificacion_sc import ext_calificacion_sc
from extract.extract_polizas import ext_polizas
from extract.extract_clientes import ext_clients

import traceback

def extract_all(ses_db_stg):
    try:
        ext_productos(ses_db_stg)
        ext_motivos_cancelacion(ses_db_stg)
        ext_calificacion_sc(ses_db_stg)
        ext_polizas(ses_db_stg)
        ext_clients(ses_db_stg)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass