USE g1dbstg;

CREATE TABLE ETL_PROCESS
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	CREATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	UPDATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE PRODUCTOS_TRA
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    ID_PRO INTEGER NOT NULL,
    NOMBRE_PRO VARCHAR(250) NOT NULL,
    ESTADO_PRO VARCHAR(8) NOT NULL,
    ETL_ID INTEGER NOT NULL
);

CREATE TABLE MOTIVOS_CANCELACION_TRA
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    ID_MOT INTEGER NOT NULL,
    DESCRIPCION_MOT VARCHAR(250) NOT NULL,
    ESTADO_MOT VARCHAR(8) NOT NULL,
    ETL_ID INTEGER NOT NULL
);

CREATE TABLE CALIFICACION_SC_TRA
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    ID_CAL INTEGER NOT NULL,
    NOMBRE_CAL VARCHAR(20) NOT NULL,
    ETL_ID INTEGER NOT NULL
);

CREATE TABLE POLIZAS_TRA
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	ID_PLZ INTEGER  NOT NULL ,
	ESTADO_PLZ VARCHAR (20)  NOT NULL ,
	ID_PRO_PLZ INTEGER  NOT NULL ,
	INICIO_VIGENCIA_PLZ DATE NOT NULL,
	FIN_VIGENCIA_PLZ DATE  NOT NULL ,
	FECHA_CAN_PLZ DATE NULL ,
	VALOR_SEGURO_PLZ DECIMAL (8,2) NOT NULL ,
	ID_MOT_PLZ INTEGER NULL,
    ETL_ID INTEGER NOT NULL
);

CREATE TABLE CLIENTES_TRA
(
	ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	ID_CLI INTEGER NOT NULL, 
	IDENTIFICACION_CLI VARCHAR (20) NOT NULL , 
	NOMBRE_CLI VARCHAR (250) NOT NULL ,
    APELLIDO_CLI VARCHAR (250) NOT NULL ,
	TIPO_CLI VARCHAR (50) NOT NULL , 
	SUCURSAL_CLI VARCHAR(50) NOT NULL , 
	APS_CLI VARCHAR (250) , 
	ID_PLZ_CLI INTEGER NOT NULL , 
	ID_CAL_CLI INTEGER NOT NULL,
    ETL_ID INTEGER NOT NULL
);