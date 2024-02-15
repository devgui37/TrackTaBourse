import polars as pl

def dataframe_frais(df_ordres:pl.DataFrame) -> pl.DataFrame:
    infos = pl.read_parquet(".\data\parquet\infos.parquet").select("isin", "ticker")
    frais = df_ordres.select("date","isin", "nombre", "montant_brut","commission","frais","montant_net")
    df_frais = frais.join(infos, on="isin")
    df_frais = df_frais.with_columns((pl.col("commission") + pl.col("frais")).alias("ponction"))
    df_frais = df_frais.with_columns((100*pl.col("ponction")/pl.col("montant_net")).alias("taux_ponction"))
    return df_frais