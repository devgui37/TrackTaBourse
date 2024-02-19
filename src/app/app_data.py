import polars as pl

def dataframe_frais(df_ordres:pl.DataFrame) -> pl.DataFrame:
    infos = pl.read_parquet("./data/parquet/infos.parquet").select("isin", "ticker")
    frais = df_ordres.select("date","isin", "nombre", "montant_brut","commission","frais","montant_net")
    df_frais = frais.join(infos, on="isin")
    df_frais = df_frais.with_columns((pl.col("commission") + pl.col("frais")).alias("ponction"))
    df_frais = df_frais.with_columns((100*pl.col("ponction")/pl.col("montant_net")).alias("taux_ponction"))
    return df_frais

def nombre_versement(df_releves:pl.DataFrame):
    versement = (
        df_releves.filter(pl.col("type").str.starts_with("VIR"))
        .select("montant")
        .sum()
        .item()
    )
    return versement

def nombre_delta_titre(df_resume:pl.DataFrame):
    delta_titre = round(
        (
            df_resume.select("valeur").sum().item()
            - df_resume.select("montant_net").sum().item()
        )
        /  df_resume.select("montant_net").sum().item()
        * 100,
        2,
    )
    return delta_titre

def nombre_performance_titre(df_resume:pl.DataFrame):
    perf_titre = round(
        df_resume.select("valeur").sum().item()
        - df_resume.select("montant_net").sum().item(),
        2,
    )
    return perf_titre

def nombre_total_dividende(df_releves:pl.DataFrame):
    total_div = (
        df_releves.filter(pl.col("type").str.starts_with("COU"))
        .select("montant")
        .sum()
        .item()
    )
    return total_div

def nombre_total_titre(df_resume:pl.DataFrame):
    total_titre = round(
        df_resume.select("valeur").sum().item(), 2
        )
    return total_titre

def nombre_total_achat(df_resume:pl.DataFrame):
    total_achat = df_resume.select("montant_net").sum().item()
    return total_achat

def nombre_total_portefeuille(versement, total_titre, total_achat, total_div):
    total_port = round(versement + (total_titre - total_achat) + total_div, 2)
    return total_port