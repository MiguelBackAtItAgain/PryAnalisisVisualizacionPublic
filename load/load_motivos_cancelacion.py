from util.functions import *
import traceback

def load_motivos_cancelacion(etl_id, ses_db_stg, ses_db_sor):
    try:

        motivos_cancelacion = get_table_regs(
            table_name='motivos_cancelacion_tra', 
            cols=['ID_MOT', 'DESCRIPCION_MOT', 'ESTADO_MOT'],
            iteration=etl_id, db_context= ses_db_stg)
        
        upsert(table_name='motivos_cancelacion', cols=['ID_MOT'], df=motivos_cancelacion, db_context=ses_db_sor)
        
    except:
        traceback.print_exc()
    finally:
        pass