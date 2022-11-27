"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re


def ingest_data():

    df = pd.read_fwf(
            "clusters_report.txt",
            widths = [8,10,20,100],
            header = None,
            names = ["cluster","cantidad_de_palabras_clave","porcentaje_de_palabras_clave", "principales_palabras_clave"],
            skip_blank_lines = False,
            converters = {"porcentaje_de_palabras_clave": 
                lambda x: x.rstrip(" %").replace(",",".")}).drop([0,1,2,3],
            axis=0
        )
    row_formated = ""
    start = 0
    for i, row in df.iterrows():
        if(isinstance(row["cluster"], str)):
            df.at[start,'principales_palabras_clave'] = row_formated
            row_formated = ""
            start = i
            row_formated += row["principales_palabras_clave"]+ " "
        elif(isinstance(row["principales_palabras_clave"], str)):
            row_formated += re.sub(' +', ' ', row["principales_palabras_clave"]).rstrip(".")
    
    
    df = df[df["cluster"].notna()]
    
    df = df.astype({
            "cluster": int,
            "cantidad_de_palabras_clave": int,
            "porcentaje_de_palabras_clave": float
        })

    return df

print(ingest_data())