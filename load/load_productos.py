from util.functions import *
import traceback

def load_productos(etl_id, ses_db_stg, ses_db_sor):
    try:

        productos = get_table_regs(
            table_name='productos_tra', 
            cols=['ID_PRO', 'NOMBRE_PRO', 'ESTADO_PRO'],
            iteration=etl_id, db_context= ses_db_stg)

        # customers_countries = get_keys(
        #     table_name='COUNTRIES', 
        #     cols=['COUNTRY_ID'], 
        #     db_context=ses_db_sor)
        
        # customers['COUNTRY_ID'] = customers['COUNTRY_ID'].apply(lambda x: customers_countries[x])
        
        upsert(table_name='productos', cols=['ID_PRO'], df=productos, db_context=ses_db_sor)

    except:
        traceback.print_exc()
    finally:
        pass