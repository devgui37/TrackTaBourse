import re
import polars as pl
from cleaning import _clean_date, clean_format_R
from scrap_pdf_avis import _extract_fichiers, _extract_text


def _scraping_table_pdf(dossier: str = ".\data") -> list[str] :
    resultats_list = []
    fichiers = _extract_fichiers(dossier, type="relevés")
    for fichier in fichiers:
        text = _extract_text(fichier)
        text2 = re.findall("ANCIENSOLDE(.*?)NOUVEAUSOLDE", text, re.DOTALL)[0].strip()
        resultats = re.findall(r"(.*?)\n", text2)
        tab = resultats[1:]
        for t in tab:
            res = re.findall(r"(\S+)\s(\S+)\s(\S+)\s?(\S+)?\s?(\S+)?", t)
            resultats_list.append(res)
    return resultats_list


def _create_dataframe(resultats_list: list[str]) -> pl.DataFrame:
    date = []
    type = []
    quantite = []
    titre = []
    montant = []
    for res in resultats_list:
        try:
            (date.append(res[0][0]),)
            (type.append(res[0][1]),)
            (quantite.append(res[0][2]),)
            (titre.append(res[0][3]),)
            montant.append(res[0][4])
        except Exception:
            pass

    df = {
        "date": date,
        "type": type,
        "quantite": quantite,
        "titre": titre,
        "montant": montant,
    }
    data = pl.DataFrame(df)
    return data


def _gestion_virement(df: pl.DataFrame) -> pl.DataFrame:
    df_vir = df.filter(pl.col("type").str.starts_with("VIR"))

    df_vir = df_vir.with_columns(
        pl.col("quantite").alias("montant"), pl.lit("").alias("quantite")
    )

    df = df.with_columns(pl.col("type").str.starts_with("VIR").alias("trieur"))
    df = df.filter(pl.col("trieur") == "false")
    df = df.drop("trieur")
    return pl.concat([df, df_vir])


def _clean_pipeline_releves(df: pl.DataFrame) -> pl.DataFrame:
    data = df.pipe(clean_format_R).pipe(_clean_date)
    return data


def create_csv_releves(dossier: str = ".\data") -> None:
    rl = _scraping_table_pdf(dossier)
    df = _create_dataframe(rl)
    df = _gestion_virement(df)
    df_final = _clean_pipeline_releves(df)
    df_final.write_parquet(r".\data\parquet\releves.parquet")
    print("Succès relevés parquet created")
    return None
