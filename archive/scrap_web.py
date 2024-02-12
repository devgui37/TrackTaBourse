import polars as pl
import numpy as np
import re
from bs4 import BeautifulSoup as BS
import requests as rq

PATTERN_COURS = re.compile(r'(\d+,\d+)')
PATTERN_CODE = re.compile(r'"symbol":"(\w+)","issuer_code"')
PATTERN_TYPE = re.compile(r'"url_type":"(\w+)","default_period"')

def extract_isin(data: str = "csv/ordres.csv") -> list[str]:
    """Récupère tous les isin issus du scraping des PDF

    Args:
        data (str, optional): Defaults to "csv/ordres_clean.csv".

    Returns:
        list[str]: 
    """
    df = pl.read_csv(data)
    isins = np.unique(df.select("isin"))
    return isins 

def scrap_cours_code(isins: list[str]) -> dict :
    """Scrap le code et le cours que chaque code ISIN

    Args:
        isins (list[str]): 

    Returns:
        dict: 
    """
    cours = []
    code = []
    type = []

    for isin in isins:
        lien = f'https://live.euronext.com/fr/product/equities/{isin}-XPAR/market-information'
        requete = rq.get(lien, timeout=1000)
        soupe = BS(requete.text)

        titre = soupe.find("title").text
        cours.append(PATTERN_COURS.search(titre).group(1))

        script = soupe.find("script", type="application/json").text
        code.append(PATTERN_CODE.search(script).group(1))
        type.append(PATTERN_TYPE.search(script).group(1))
    
    df = {"isin": list(isins), "code": code, "cours": cours, "type": type}
    return df

def clean_write_csv(df:dict) -> None:
    """Convertit en polars, clean, transforme en CSV

    Args:
        df (dict):
    """
    data = pl.DataFrame(df)

    data_clean = data.with_columns(
        pl.col("cours").str.replace(",", ".").cast(pl.Float64)
    )
    data_clean.write_csv(r".\csv\cours.csv", separator=",")
    
def web_pipeline(data: str = "csv/ordres.csv") -> None:
    """Combine les fonctions du module scrap_web

    Args:
        data (str, optional):  Defaults to "csv/ordres.csv".
    """
    isins = extract_isin(data)
    df = scrap_cours_code(isins)
    clean_write_csv(df)
    return print("Succès")