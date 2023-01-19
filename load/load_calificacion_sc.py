from util.functions import *
import traceback

def load_calificacion(etl_id, ses_db_stg, ses_db_sor):
    try:

        calificacion_sc = get_table_regs(
            table_name='calificacion_sc_tra', 
            cols=['ID_CAL', 'NOMBRE_CAL'],
            iteration=etl_id, db_context= ses_db_stg)

        # customers_countries = get_keys(
        #     table_name='COUNTRIES', 
        #     cols=['COUNTRY_ID'], 
        #     db_context=ses_db_sor)
        
        # customers['COUNTRY_ID'] = customers['COUNTRY_ID'].apply(lambda x: customers_countries[x])
        
        upsert(table_name='calificacion_sc', cols=['ID_CAL'], df=calificacion_sc, db_context=ses_db_sor)

    except:
        traceback.print_exc()
    finally:
        pass