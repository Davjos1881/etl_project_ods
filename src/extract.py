import pandas as pd

def extract_incautaciones(path):
    df = pd.read_csv(path, sep=",", encoding="utf-8")

    df.columns = [
        "anio",
        "departamento",
        "municipio",
        "lugar_decomiso",
        "situacion",
        "autoridad_que_incauto",
        "tipo_especie",
        "nombre_comun",
        "nombre_cientifico",
        "cantidad"
    ]

    return df