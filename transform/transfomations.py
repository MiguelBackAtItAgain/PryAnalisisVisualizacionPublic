from datetime import datetime

def join_2_strings(string1, string2):
    return f"{string1} {string2}"

def separate_2_strings(string):
    return string.split()

def obt_estado_producto(est):
    if est == 'A':
        return 'ACTIVO'
    elif est == 'I':
        return 'INACTIVO'
    else:
        return 'N/D'

def obt_estado_poliza(est):
    if est=='ACTIVA':
        return 'A'
    elif est =='CANCELADA':
        return 'C'
    elif est=='SUSPENDIDA':
        return 'S'
    else:
        return 'N/D'

def obt_tipo_cliente(est):
    if est == 'N':
        return 'NATURAL'
    elif est == 'J':
        return 'JURIDICA'
    elif est == 'E':
        return 'ESTATAL'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    return datetime.strptime(date_string, '%d-%b-%y')

def obt_date_2(date_string):
    if date_string is None:
        return None
    else:
        return datetime.strptime(date_string, '%m/%d/%Y')

def str_2_int(string1):
    if type(string1) is str:
        num = int(string1)
    elif type(string1) is int:
        num = string1
    else:
        num = 30
    return num

def str_2_float(string1):
    if type(string1) is str:
        num = float(string1)
    elif type(string1) is float:
        num = string1
    else:
        num = None
    return num