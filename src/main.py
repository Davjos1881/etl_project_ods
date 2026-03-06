import pandas as pd
from tabulate import tabulate
from log import log_progress
from extract import extract_incautaciones
from transform import transform_data
from load import save_dimensions_to_csv, load_to_dw

<<<<<<< HEAD
log_file = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto_1\etl_project_ods-main\etl_project_ods-main\logs\log_file.txt'

target_file = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto_1\etl_project_ods-main\etl_project_ods-main\transformed'

data_path = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto_1\etl_project_ods-main\etl_project_ods-main\raw\incautaciones.csv'
=======
log_file = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\logs\log_file.txt'
target_file = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\transformed'
data_path = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\raw\incautaciones.csv'
>>>>>>> 701e10fe1c81ff506cec95086e0c4158bbb7db4e

def main():
    # ETL process
    log_progress('Starting ETL process', log_file)

    # Extract
    log_progress('Extract phase started', log_file)

    df_incautaciones = extract_incautaciones(data_path)
    print(tabulate(df_incautaciones.head(), headers='keys', tablefmt='psql'))

    log_progress("Extract phase complete", log_file)

    # Transform
    log_progress('Transform phase started', log_file)

    df_transform = transform_data(df_incautaciones)

    print("\nDIM_TIEMPO")
    print(tabulate(df_transform["dim_tiempo"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_UBICACION")
    print(tabulate(df_transform["dim_ubicacion"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_ESPECIE")
    print(tabulate(df_transform["dim_especie"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_AUTORIDAD")
    print(tabulate(df_transform["dim_autoridad"].head(), headers='keys', tablefmt='psql'))

    print("\nFACT_INCAUTACIONES")
    print(tabulate(df_transform["fact_incautaciones"].head(), headers='keys', tablefmt='psql'))

    log_progress('Transform phase complete', log_file)

    # Load
    log_progress('Load phase started', log_file)

    save_dimensions_to_csv(
        target_file,
        dim_tiempo=df_transform["dim_tiempo"],
        dim_ubicacion=df_transform["dim_ubicacion"],
        dim_especie=df_transform["dim_especie"],
        dim_autoridad=df_transform["dim_autoridad"],
        fact_incautaciones=df_transform["fact_incautaciones"]
    )

    load_to_dw(df_transform)
    log_progress('Load phase complete', log_file)

    # ETL process
    log_progress('ETL process finished successfully', log_file)


if __name__ == "__main__":
    main()