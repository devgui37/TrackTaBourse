import yfinance as yf
import polars as pl
import numpy as np


def get_list_isin(csv_ordres: str = "./data/parquet/ordres.parquet") -> list[str]:
    """Recupére tous les ISIN traité dans les ordres

    Args:
        csv_ordres (str, optional): _description_. Defaults to "./data/parquet/ordres.parquet".

    Returns:
        list[str]: _description_
    """
    list_isin = list(np.unique(pl.read_parquet(csv_ordres).select("isin")))
    return list_isin


def get_cotation_isin(isin: str) -> pl.DataFrame:
    """A partir des codes ISIN récupère cotation et ticker de chaque titre

    Args:
        isin (str): code ISIN

    Returns:
        pl.DataFrame: 
    """
    ticker = yf.Ticker(isin)
    history = ticker.history(start="2022-11-15", interval="1d")

    df = {"date": history.index, isin: history.Close}
    data = pl.DataFrame(df)
    return data

def df_creata_all_cotation(liste_isin:list[str]) -> None:
    df_all_cotation = None
    for isin in liste_isin:
        df = get_cotation_isin(isin)
        
        if df_all_cotation is None:
            df_all_cotation = df
        else:
            df_all_cotation = df_all_cotation.join(df, on="date", how="left")
    df_all_cotation.write_parquet("./data/parquet/cotation.parquet")
    return None

def join_cotation_ordres(isin: str):
    """Permet de join les cotations avec leur ordre

    Args:
        data_isin (pl.DataFrame): _description_
        isin (str): _description_

    Returns:
        _type_: _description_
    """
    df_cotation_isin = pl.read_parquet("./data/parquet/cotation.parquet").select("date", isin)
    df_ordres = pl.read_parquet("./data/parquet/ordres.parquet")
    df_ordres_isin = df_ordres.filter(pl.col("isin") == isin)
    df_ordres_isin = df_ordres_isin.select("date", "nombre", "montant_net")
    df_join = df_cotation_isin.join(df_ordres_isin, on="date", how="left")
    return df_join


def join_cleaning(df_join: pl.DataFrame, isin: str) -> None:
    df_join = df_join.with_columns(pl.col("nombre").fill_null(strategy="zero"),
                                   pl.col("montant_net").fill_null(strategy="zero"))
    df_join = df_join.with_columns(pl.cumsum("nombre")
                                   .alias("total_nombre"))
    dd = df_join.with_columns((pl.col("total_nombre") * pl.col(isin))
                              .alias("valeur"))
    dd = dd.with_columns(pl.col("montant_net")
                         .cumsum())
    return dd 


def join_all_df_cotation(liste_isin: str, variable : str) -> pl.DataFrame:
    df_all = None

    for isin in liste_isin:
        df = join_cleaning(join_cotation_ordres(isin), isin).select("date", variable)
        df = df.rename({variable: isin})
        if df_all is None:
            df_all = df
        else:
            df_all = df_all.join(df, on="date", how="left")

    df_all = df_all.with_columns(
        pl.fold(
            0, lambda sum_total, colonne: sum_total + colonne, pl.all().exclude("date")
        ).alias("total")
    )
    df_all = df_all.rename({"total": str("total_" + variable)})
    return df_all


def df_evolution_portefeuille(liste_isin: list[str]) -> pl.DataFrame:
    total_valeur = join_all_df_cotation(liste_isin, "valeur").select("date", "total_valeur")
    total_montant_net = join_all_df_cotation(liste_isin, "montant_net").select("date", "total_montant_net")
    df = total_montant_net.join(total_valeur, on = "date", how = "left")
    df = df.with_columns(
        (100 * (pl.col("total_valeur") - pl.col("total_montant_net")) / pl.col("total_montant_net"))
        .alias("variation"),
        (pl.col("total_valeur") - pl.col("total_montant_net"))
        .alias("variation_eur"),
    )
    return df

def parquet_info_isin():
    list_isin = get_list_isin()
    ticker = []
    name = []
    type = []
    for isin in list_isin:
        titre = yf.Ticker(isin)
        ticker.append(titre.ticker)
        name.append(titre.info["longName"])
        type.append(titre.get_info()["quoteType"])
    df = {"isin" : list_isin,
        "ticker" : ticker,
        "name" : name,
        "type": type}
    data = pl.DataFrame(df)
    data.write_parquet("./data/parquet/infos.parquet")
    return print("Infos parquet créée")