import csv
from faker import Faker
import pandas as pd
import traceback

def generate_random_client_names():
    fake_name_gen = Faker()
    return fake_name_gen.name()

def generate_random_id_numbers():
    fake_id_num = Faker()
    return fake_id_num.unique.random_int(min=1000000000, max=9999999999)

def generate_random_company_names():
    fake_company_name =  Faker()
    return fake_company_name.company()

def ext_clients(ses_db_stg):
    try:
        path = 'csvs/CLIENTES.csv'
        clients_dic = {
            "id_cli" : [],
            "identificacion_cli" : [],
            "nombre_cli" : [],
            "tipo_cli" : [],
            "sucursal_cli" : [],
            "aps_cli" : [],
            "id_plz_cli" : [],
            "id_cal_cli" : []
        }

        clients = pd.read_csv(path)

        """El código comentado sirvió para actualizar los valores reales a aquellos generados por Faker.
        A pesar de que se continúa """
        # client_names = []
        # client_id_nums = []
        # client_aps = []
        # for i in range(len(clients)):
        #     client_names.append(str(generate_random_client_names()))
        #     client_id_nums.append(str(generate_random_id_numbers()))
        #     client_aps.append(str(generate_random_company_names()))

        # clients["NOMBRE_CLI"] = client_names
        # clients["IDENTIFICACION_CLI"] = client_id_nums
        # clients["APS_CLI"] = client_aps
        

        # clients.to_csv("csvs/clients.csv", index=False)
        #Después, se continuó con un proceso de transformación en Excel para que 
        #tengamos la versión final del documento CLIENTES.csv

        if not clients.empty:
            for id, cedula, nombre, tipo, sucursal, aps, poliza, calif \
                in zip(clients["ID_CLI"], clients["IDENTIFICACION_CLI"],
                    clients["NOMBRE_CLI"], clients["TIPO_CLI"], clients["SUCURSAL_CLI"],
                    clients["APS_CLI"], clients["ID_PLZ_CLI"], clients["ID_CAL_CLI"]):
                    
                    clients_dic["id_cli"].append(id),
                    clients_dic["identificacion_cli"].append(generate_random_id_numbers()),
                    clients_dic["nombre_cli"].append(generate_random_client_names()),
                    clients_dic["tipo_cli"].append(tipo),
                    clients_dic["sucursal_cli"].append(sucursal),
                    clients_dic["aps_cli"].append(generate_random_company_names()),
                    clients_dic["id_plz_cli"].append(poliza),
                    clients_dic["id_cal_cli"].append(calif)
        
        if clients_dic["id_cli"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE clientes_ext")
            df_clients = pd.DataFrame(clients_dic)
            df_clients.to_sql('clientes_ext', ses_db_stg, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass
    