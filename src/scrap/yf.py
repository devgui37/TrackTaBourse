import yfinance as yf
import polars as pl
import numpy as np


def liste_isin(csv_ordres: str = "./data/parquet/ordres.parquet") -> list[str]:
    list_isin = list(np.unique(pl.read_parquet(csv_ordres).select("isin")))
    return list_isin


def history_isin(isin: str) -> pl.DataFrame:
    """A partir des codes ISIN récupère cotation et ticker de chaque titre

    Args:
        isin (str): code ISIN

    Returns:
        pl.DataFrame: 
    """
    ticker = yf.Ticker(isin)
    history = ticker.history(start="2022-11-15", interval="1d")

    df = {"date": history.index, "cotation": history.Close, "ticker" : ticker.ticker}
    data = pl.DataFrame(df)
    return data


def join_isin_ordres(data_isin: pl.DataFrame, isin: str):
    df_ordres = pl.read_parquet("./data/parquet/ordres.parquet")
    df_isin = df_ordres.filter(pl.col("isin") == isin)
    df_isin = df_isin.select("date", "nombre", "montant_net")
    df_cours = data_isin.join(df_isin, on="date", how="left")
    return df_cours


def clean_join(df_cours: pl.DataFrame, isin: str) -> pl.DataFrame:
    df_cours = df_cours.with_columns(pl.col("nombre").fill_null(strategy="zero"))
    df_cours = df_cours.with_columns(pl.cumsum("nombre").alias("total_nombre"))
    dd = df_cours.with_columns(
        (pl.col("total_nombre") * pl.col("cotation")).alias("valeur")
    )
    dd = dd.with_columns(pl.col("montant_net").fill_null(strategy="zero"))
    dd = dd.with_columns(pl.col("montant_net").cumsum())
    dd.write_parquet(f"./data/parquet/{isin}.parquet")
    return print("Succès parquet")


def merged(variable: str, liste_isin: str, prefixe: str = "") -> pl.DataFrame:
    merged_df = None

    for isin in liste_isin:
        df = pl.read_parquet(f"./data/parquet/{isin}.parquet").select("date", f"{variable}")
        df = df.rename({f"{variable}": f"{prefixe}{isin}"})
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.join(df, on="date", how="outer")

    merged_df = merged_df.with_columns(
        pl.fold(
            0, lambda sum_total, colonne: sum_total + colonne, pl.all().exclude("date")
        ).alias("total")
    )
    return merged_df


def evolution_port(liste_isin: list[str]) -> pl.DataFrame:
    merged_df = merged("valeur", liste_isin)
    merged_df_PR = merged("montant_net", liste_isin, "PR_")
    datoum = merged_df.join(merged_df_PR, on="date", how="left")
    datoum = datoum.fill_null(strategy="backward")
    datoum = datoum.with_columns(
        (100 * (pl.col("total") - pl.col("total_right")) / pl.col("total_right")).alias(
            "variation"
        ),
        (pl.col("total") - pl.col("total_right")).alias("variation_eur"),
    )
    return datoum


def yf_pipeline(dossier: str = "./data/parquet/ordres.parquet") -> None:
    list_isin = liste_isin(dossier)
    for isin in list_isin:
        data = history_isin(isin)
        data_join = join_isin_ordres(data, isin)
        clean_join(data_join, isin)
    return None

def ticker():
    list_isin = liste_isin()
    ticker = []
    name = []
    for isin in list_isin:
        titre = yf.Ticker(isin)
        ticker.append(titre.ticker)
        name.append(titre.info["longName"])
    df = {"isin" : list_isin,
        "ticker" : ticker,
        "name" : name}
    data = pl.DataFrame(df)
    data.write_parquet("./data/parquet/infos.parquet")
    return print("Infos parquet créée")