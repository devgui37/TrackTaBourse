"""Module de scraping PDF boursorama"""

#import pdfplumber
import re
import os
import glob
import polars as pl
from pypdf import PdfReader

def _extract_fichiers(dossier: str, type: str) -> list[str]:
    """Récupère le chemin des fichiers PDF à scraper

    Args:
        dossier (str): _description_

    Returns:
        list[str] : liste avec les nom des fichiers
    """
    if type == "avis":
        noms_fichiers = glob.glob(os.path.join(dossier, "A*"))
    elif type == "relevés":
        noms_fichiers = glob.glob(os.path.join(dossier, "R*"))
    return noms_fichiers


def _extract_text(nom_fichier: str) -> str:
    """Récupère le contenu du PDF

    Args:
        nom_fichier (str): chemin

    Returns:
        str: contenu du PDF
    """
    reader = PdfReader(nom_fichier)
    page = reader.pages[0]
    text = page.extract_text()
    return text


def _re_date_name_number(text: str) -> tuple[str, str, str]:
    """Renvoie
    - date d'achat
    - titre acheté
    - nombre acheté

    Args:
        text (str): contenu du PDF à scraper

    Returns:
    """
    pattern = re.compile(r"exécution\n(\d{2}/\d{2}/\d{4})\n(\d{2}:\d{2}:\d{2})(\d+) (.+) Référence")
    match = pattern.search(text)

    date = match.group(1) if match else None
    name = match.group(4) if match else None
    number = match.group(3) if match else None

    return date, name, number


def _re_heure(text: str) -> str:
    """Renvoie
    - heure d'achat (exécution de l'ordre)

    Args:
        text (str): contenu du PDF à scraper

    Returns:
    """
    pattern = re.compile(r"(\d{2}:\d{2}:\d{2})")
    match = pattern.search(text)
    heure = match.group(1) if match else None
    return heure


def _re_montants(text: str) -> tuple[str,str,str,str] | tuple[str,str,str]:
    """Récupère le montant engagé dans l'achat
    Gére les cas où le case "Frais" est remplit ou vide.

    Args:
        text (str): contenu du PDF à scraper

    Returns:

    """
    try:
        pattern = re.compile(
            r"compte\n(\d+,\d+)EUR\s?(\d+,\d+)EUR\s?(\d+,\d+)\s?EUR\s?\s?(\d+,\d+)\s?EUR"
        )
        match = pattern.search(text)
        montant_brut = match.group(1) if match else None
        commission = match.group(2) if match else None
        frais = match.group(3) if match else None
        montant_net = match.group(4)
    except AttributeError:
        pattern = re.compile(
            r"compte\n(\d+,\d+)\s?EUR\s?(\d+,\d+)\s?EUR\s?\s?(\d+,\d+)\s?EUR"
        )
        match = pattern.search(text)
        montant_brut = match.group(1) if match else None
        commission = match.group(2) if match else None
        frais = 0
        montant_net = match.group(3) if match else None
    return montant_brut, commission, frais, montant_net


def _re_cours(text: str) -> str:
    """Récupère le cours d'éxécution de l'achat

    Args:
        text (str): ontenu du PDF à scraper

    Returns:
    """
    cours_pattern = re.compile(r"exécuté\s?:\s?(\d+,\d+)\s?EUR")
    cours_match = cours_pattern.search(text)
    cours = cours_match.group(1)
    return cours


def _re_isin(text: str) -> str:
    """Récupère le code ISIN du titre

    Args:
        text (str): ontenu du PDF à scraper

    Returns:
    """
    pattern = re.compile(r"Code\s?ISIN\s?:\s?(.+)\s?Cours")
    match = pattern.search(text)
    isin = match.group(1)
    return isin


def re_pipeline(dossier: str = "./data/"):
    """Scrap toute les infos d'un PDF

    Args:
        dossier (str): Dossier contenant les PDF à scraper
    """
    nom_fichiers = _extract_fichiers(dossier, type="avis")
    info_pdf = 1
    datoum = None
    for fichier in nom_fichiers:
        text = _extract_text(fichier)
        d = {
            "date": _re_date_name_number(text)[0],
            "heure": _re_heure(text),
            "produit": _re_date_name_number(text)[1],
            "isin": _re_isin(text).strip(),
            "nombre": _re_date_name_number(text)[2],
            "cours": _re_cours(text),
            "montant_brut": _re_montants(text)[0],
            "commission": _re_montants(text)[1],
            "frais": _re_montants(text)[2],
            "montant_net": _re_montants(text)[3],
        }
        if datoum is None:
            datoum = pl.DataFrame([d])
        else:
            df = pl.DataFrame([d])
            df = df.with_columns(pl.col("frais").cast(pl.Utf8))
            datoum = datoum.vstack(df)
        print(f"PDF n°{info_pdf} scrapé !")
        info_pdf += 1

    datoum.write_parquet("./data/parquet/ordres.parquet")
    print("Fichiers PDF scrapés avec succès.")
    return None
