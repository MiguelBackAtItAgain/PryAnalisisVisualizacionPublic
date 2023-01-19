USE g1dbstg;

CREATE TABLE PRODUCTOS_EXT
(
    ID_PRO VARCHAR(5) NOT NULL,
    NOMBRE_PRO VARCHAR(250) NOT NULL,
    ESTADO_PRO VARCHAR(8) NOT NULL
);

CREATE TABLE MOTIVOS_CANCELACION_EXT
(
    ID_MOT VARCHAR(5) NOT NULL,
    DESCRIPCION_MOT VARCHAR(250) NOT NULL,
    ESTADO_MOT VARCHAR(8) NOT NULL
);

CREATE TABLE CALIFICACION_SC_EXT
(
    ID_CAL VARCHAR(2) NOT NULL,
    NOMBRE_CAL VARCHAR(20) NOT NULL
);

CREATE TABLE POLIZAS_EXT
    (
     ID_PLZ VARCHAR(10)  NOT NULL ,
     ESTADO_PLZ VARCHAR (20)  NOT NULL ,
     ID_PRO_PLZ VARCHAR (20)  NOT NULL ,
     INICIO_VIGENCIA_PLZ VARCHAR(30) NOT NULL,
     FIN_VIGENCIA_PLZ VARCHAR(30)  NOT NULL ,
     FECHA_CAN_PLZ VARCHAR (20) NULL ,
     VALOR_SEGURO_PLZ VARCHAR (20)  NOT NULL ,
     ID_MOT_PLZ VARCHAR (20) NULL
    )
;

CREATE TABLE CLIENTES_EXT
    ( 
     ID_CLI VARCHAR(10)  NOT NULL, 
     IDENTIFICACION_CLI VARCHAR (20) NOT NULL , 
     NOMBRE_CLI VARCHAR (250) NOT NULL , 
     TIPO_CLI VARCHAR (50) NOT NULL , 
     SUCURSAL_CLI VARCHAR(50) NOT NULL , 
     APS_CLI VARCHAR (250) , 
     ID_PLZ_CLI VARCHAR(10) NOT NULL , 
     ID_CAL_CLI VARCHAR(10) NOT NULL
    )
;