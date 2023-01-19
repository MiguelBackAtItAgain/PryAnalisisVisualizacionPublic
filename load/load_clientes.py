from util.functions import *
import pandas as pd
import traceback

def load_clients(etl_id, ses_db_stg, ses_db_sor):
    try:
        clientes = get_table_regs(
            table_name='clientes_tra',
            cols = ['ID_CLI', 'IDENTIFICACION_CLI', 'NOMBRE_CLI', 'APELLIDO_CLI', 'TIPO_CLI', 'SUCURSAL_CLI', 'APS_CLI', 
                    'ID_PLZ_CLI', 'ID_CAL_CLI'],
            iteration=etl_id,
            db_context = ses_db_stg)
        
        calificacion_cliente = get_keys(
            table_name='calificacion_sc',
            cols = ['ID_CAL'],
            db_context=ses_db_sor
        )

        cliente_poliza = get_keys(
            table_name='polizas',
            cols = ['ID_PLZ'],
            db_context=ses_db_sor
        )

        clientes['ID_CAL_CLI'] = clientes['ID_CAL_CLI'].apply(lambda x : calificacion_cliente[x])
        clientes['ID_PLZ_CLI'] = clientes['ID_PLZ_CLI'].apply(lambda x : cliente_poliza[x])
        

        upsert(table_name='clientes', cols=['ID_CLI'], df=clientes, db_context=ses_db_sor)

    except:
        traceback.print_exc()
    finally:
        pass
