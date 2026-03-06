**Proyecto ETL - ODS 15: Vida de Ecosistemas Terrestres**

Por:

-Jose David Santa

-Valentina Morales

-Brayan Stiven Tigreros

**Objetivo del proyecto**

Diseñar e implementar un pipeline ETL de producción sobre datos de incautaciones de fauna silvestre en los departamentos de Risaralda y Caldas, Colombia, en el marco del Objetivo de Desarrollo Sostenible 15 (Vida de Ecosistemas Terrestres). El sistema extrae datos crudos en CSV, los transforma aplicando limpieza y modelado dimensional, y los carga en un Data Warehouse MySQL para su análisis mediante dashboards en Power BI.

El tráfico ilegal de especies en Colombia es un crimen ambiental y animal que implica la captura, comercialización, transporte ilícito de fauna y flora silvestre, siendo este uno de los negocios ilícitos más rentables del mundo. Esta actividad afecta gravemente la biodiversidad, aumenta la extinción de especies protegidas, reduce la poblacion de otras especies y aumenta la introduccion de especies exóticas al ecosistema que pueden llegar a ser perjudicial para el ecosistema local. Esta actividad tiende a ser desarrollada para utilizar a las especies como mascotas, carne, huevos o por sus partes como lo son las pieles, a menudo esta actividad provoca alta mortalidad en los animales en cuestión.

La incautación de animales en Colombia es crucial para combatir el tráfico ilegal y detener la crueldad animal, con el fin de asegurar el equilibrio de los ecosistemas y la protección de los seres vivos de nuestra región, es por ello que dentro del pais se generan normativas y programas de incautación como en el caso de la gobernacion de Risaralda y Caldas que son departamentos con gran presencia de tráfico ilegal de fauna silvestre, impulsado por la biodiversidad de la zona.

**Dataset**

El archivo incautaciones.csv contiene aproximadamente 12,836 registros con 10 atributos: año del evento, departamento, municipio, lugar del decomiso, situación (INCAUTACIÓN, ENTREGA VOLUNTARIA o HALLAZGO), autoridad que intervino, tipo de especie, nombre común, nombre científico y cantidad de individuos. El año venía serializado como float (2.008) y fue corregido durante la transformación. Se identificaron nulos en municipio (31), autoridad (34), tipo de especie (10), nombre común (738) y nombre científico (703), todos tratados en la fase de transform.

## Modelo Dimensional
**Definición de la granularidad**

| Dimensión              | Atributo 1              | Atributo 2      | Atributo 3        |
|------------------------|------------------------|-----------------|------------------|
| Especie dimensión      | Nom tipo especie       | Nombre común   | Nombre científico |
| Ubicación dimensión    | Departamento           | Municipio      | Lugar decomiso    |
| Autoridad dimensión    | Autoridad que incautó  |                 |                  |
| Tiempo dimensión       | Año                    |                 |                  |
| Fact Table             | Situación              | Cantidad        |                  |


Un registro por evento de incautación o entrega de fauna silvestre, identificado por la combinación de año, ubicación (departamento + municipio + lugar), especie (tipo + nombre común + nombre científico) y autoridad interviniente.

**Decisiones del Esquema Estrella**

La fact table fact_incautaciones contiene dos medidas: 

cantidad (individuos) y situacion (tipo de evento). 

Se mantuvo en la fact table y no como dimensión porque solo tiene 3 valores distintos y varía por evento individual. 

Las cuatro dimensiones son dim_tiempo (año), dim_ubicacion (departamento, municipio, lugar), dim_especie (tipo, nombre común, nombre científico) y dim_autoridad (entidad interviniente).

Todos los surrogate keys se generan con reset_index() + 1 en pandas antes de la carga.

lugar_decomiso se integró dentro de dim_ubicacion por formar parte de la jerarquía geográfica del evento.

Imagen del Esquema Estrella:

<img width="1187" height="792" alt="image" src="https://github.com/user-attachments/assets/aa1fe6eb-c739-4937-b72b-f2c863d80f48" />

**Lógica ETL**

En la fase de extracción se lee el CSV con pandas usando separador coma y encoding UTF-8, asignando los nombres de columna definidos. 

En la transformación se corrige el año multiplicando por 1000, se imputan los nulos con el valor DESCONOCIDO, se estandariza todo el texto con strip y upper, se construyen las cuatro dimensiones eliminando duplicados y se genera la fact table mediante merges secuenciales con cada dimensión para obtener las llaves foráneas. 

En la carga, las dimensiones se insertan usando INSERT IGNORE con tabla temporal para evitar duplicados, y la fact table aplica un anti-join contra las llaves ya existentes en el DW para garantizar idempotencia del pipeline.

**Supuestos de Calidad de Datos**

Los nulos en nombre común y científico se interpretan como registros sin identificación taxonómica completa al momento de la incautación. Los nulos en autoridad corresponden a eventos sin registro de la entidad interviniente. El año en formato 2.008 se considera un problema de serialización del CSV original y no un error en los datos. La capitalización inconsistente entre registros (por ejemplo "Centro Rehabilitacion AVES RAPACES" vs "CARDER") se normaliza a mayúsculas para garantizar consistencia en las dimensiones.

**Cómo ejecutar el proyecto**

Primero instala las dependencias

    pip install -r requirements.txt

Luego crea la base de datos en MySQL ejecutando el archivo **incautaciones.sql** para crear las tablas. 

En main.py actualiza las tres rutas: log_file, target_file y data_path según tu entorno local.

En load.py reemplaza usuario y contraseña en la cadena de conexión de SQLAlchemy.

Finalmente ejecuta el main.py y verás en consola el preview tabular de cada dimensión y la fact table, además del log de cada fase.

La consola deberia dar esta salida:

<img width="589" height="71" alt="image" src="https://github.com/user-attachments/assets/7307a195-eaec-403e-bed6-8d3360db8d64" />

En MySQL Workbench, comprueba que se cargaron los registros, abre un SQL script y escribe el comando:

    SELECT * FROM incautaciones_dw.fact_incautaciones;

Al ejecutarlo, deberias ver la tabla de hechos juntos con las llaves foraneas de las dimensiones, tambien puedes comprobar si se cargaron los registros de las dimensiones

    SELECT * FROM incautaciones_dw.dim_nombreDeDimension;

Por ejemplo, para la dimension autoridad:

    SELECT * FROM incautaciones_dw.dim_autoridad;

<img width="883" height="338" alt="image" src="https://github.com/user-attachments/assets/412a51d0-31c6-4646-9ae7-5b824a47ad4d" />

**KPIs del Dashboard**

El dashboard en Power BI responde a ocho preguntas o KPIs que serán importantes para la creación o implementación de estrategias para combatir el trafico de especies en los departamentos de Caldas y Risaralda, estas serán:

1. ¿Cuál es la especie con más individuos incautados?
2. ¿En qué municipio ocurren más eventos?
3. ¿Qué autoridad lidera las intervenciones?
4. ¿Cómo se distribuyen los eventos entre incautación, entrega voluntaria y hallazgo?
5. ¿Cómo ha evolucionado el volumen de fauna incautada entre 2008 y 2021?
6. ¿Cuál es el nombre común más frecuente?
7. ¿Dónde se concentran los hallazgos por lugar?
8. ¿Cuál es el total acumulado de individuos registrados en el DW?

Cada una de ellas tendra una importancia que explicara su implementación dentro del BI, este seguira el orden respectivo antes dato a los KPIs

1. 

Ahora bien, para conectar el MySQL Workbench al Power BI necesitaremos un extra:

MySQL Connector/ODBC (version 8.x/64bit).

Esto es necesario puesto que este proyecto consume el Data Warehouse desde Power BI mediante una fuente de datos ODBC configurada con el driver de MySQL.

Creando el DSN
Abre ODBC Data Sources (64-bit) en Windows, ve a la pestaña System DSN y haz clic en Add.

Selecciona MySQL ODBC 8.0 Unicode Driver y configura los siguientes campos: 

  Data Source Name: incautaciones_dw_dsn
  
  TCP/IP: Server localhost
  
  Port: 3306
  
  User y Password: tus credenciales de MySQL
  
  Database: incautaciones_dw. 

Haz clic en Test para verificar la conectividad y luego OK para guardar.

El repositorio incluye el archivo .pbix con el reporte prearmado. Para conectarlo a tu Data Warehouse local después de clonar el proyecto, abre el archivo ubicado en projecto_etl_ods/diagrams/power_bi_kpis/kpis_incautaciones.pbix con Power BI Desktop. 

Luego ve a Inicio → Transformar Datos → Configuración de origen de datos

Selecciona la fuente ODBC, haz clic en Editar Permisos e ingresa tus credenciales de MySQL si se solicitan. 

Confirma que el DSN utilizado sea incautaciones_dw_dsn (o el que tengas creado) y refresca el dataset desde Inicio → Actualizar.

Después de refrescar, el reporte mostrará los siguientes KPIs: 

-cantidad de animales incautados por especie

-incautados por año

-proporción por situación (incautación, entrega voluntaria y hallazgo)

-incautados por autoridad

-total de individuos incautados. 

Los valores del reporte deben coincidir con el contenido de la base de datos incautaciones_dw tras ejecutar el pipeline completo con python main.py. Un ejemplo de como deberia lucir el reporte:

<img width="1439" height="809" alt="Captura de pantalla 2026-03-05 225552" src="https://github.com/user-attachments/assets/742d1e5c-5a2d-4902-95d6-f7b87eee44b9" />

