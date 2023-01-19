from util.functions import *
import traceback

def load_polizas(etl_id, ses_db_stg, ses_db_sor):
    try:
        polizas = get_table_regs(
            table_name='polizas_tra', 
            cols=['ID_PLZ', 'ESTADO_PLZ', 'ID_PRO_PLZ', 'INICIO_VIGENCIA_PLZ', 'FIN_VIGENCIA_PLZ', 'FECHA_CAN_PLZ', 'VALOR_SEGURO_PLZ', 'ID_MOT_PLZ'],
            iteration=etl_id,
            db_context= ses_db_stg)

        polizas_productos = get_keys(
            table_name='productos', 
            cols=['ID_PRO'], 
            db_context=ses_db_sor)

        polizas_motivos = get_keys(
            table_name='motivos_cancelacion', 
            cols=['ID_MOT'], 
            db_context=ses_db_sor)

        polizas['ID_PRO_PLZ'] = polizas['ID_PRO_PLZ'].apply(lambda x: polizas_productos[x])
        polizas['ID_MOT_PLZ'] = polizas['ID_MOT_PLZ'].apply(lambda x: polizas_motivos[x])

        upsert(table_name='polizas', cols=['ID_PLZ'], df=polizas, db_context=ses_db_sor)

    except:
        traceback.print_exc()
    finally:
        pass