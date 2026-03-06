USE incautaciones_dw;

CREATE TABLE dim_tiempo (
    tiempo_key INT PRIMARY KEY,
    anio       INT NOT NULL
);

CREATE TABLE dim_ubicacion (
    ubicacion_key  INT PRIMARY KEY,
    departamento   VARCHAR(100) NOT NULL,
    municipio      VARCHAR(100) NOT NULL,
    lugar_decomiso VARCHAR(255) NOT NULL
);

CREATE TABLE dim_especie (
    especie_key       INT PRIMARY KEY,
    tipo_especie      VARCHAR(100) NOT NULL,
    nombre_comun      VARCHAR(255) NOT NULL,
    nombre_cientifico VARCHAR(255) NOT NULL
);

CREATE TABLE dim_autoridad (
    autoridad_key         INT PRIMARY KEY,
    autoridad_que_incauto VARCHAR(255) NOT NULL
);

CREATE TABLE fact_incautaciones (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    tiempo_key     INT NOT NULL,
    ubicacion_key  INT NOT NULL,
    especie_key    INT NOT NULL,
    autoridad_key  INT NOT NULL,
    situacion      VARCHAR(100) NOT NULL,
    cantidad       INT NOT NULL,

    FOREIGN KEY (tiempo_key)    REFERENCES dim_tiempo(tiempo_key),
    FOREIGN KEY (ubicacion_key) REFERENCES dim_ubicacion(ubicacion_key),
    FOREIGN KEY (especie_key)   REFERENCES dim_especie(especie_key),
    FOREIGN KEY (autoridad_key) REFERENCES dim_autoridad(autoridad_key)
);