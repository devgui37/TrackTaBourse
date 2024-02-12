import streamlit as st
import polars as pl
from src.app.app_function import background_color_table
import yfinance as yf


def ordres_groupby(df_ordres: pl.DataFrame) -> pl.DataFrame:
    df_join = (
        df_ordres.group_by("isin")
        .agg(
            pl.col("produit").first(),
            pl.col("montant_brut").sum(),
            pl.col("commission").sum(),
            pl.col("frais").sum(),
            pl.col("montant_net").sum(),
            pl.col("nombre").sum(),
            ((pl.col("cours_unit") * pl.col("nombre")).sum() / pl.sum("nombre"))
            .round(2)
            .alias("PRU"),
        )
        .sort("montant_net", descending=True)
    )
    return df_join


def tab_gb_cotation(df_ordres: pl.DataFrame) -> pl.DataFrame:
    table = ordres_groupby(df_ordres)
    liste_isin = [isin[0] for isin in table.iter_rows()]
    cotation = []
    code = []
    day_variation = []
    quote_type = []

    for isin in liste_isin:
        titre = yf.Ticker(isin)
        cotation.append(titre.history(period="1mo").tail(1)["Close"].item())
        code.append(titre.ticker)
        quote_type.append(titre.get_info()["quoteType"])

        lasts_days = titre.history(period="1mo").tail(2)
        day_variation.append(
            (lasts_days["Close"][1] - lasts_days["Close"][0])
            / lasts_days["Close"][0]
            * 100
        )

    table = table.with_columns(
        code=pl.lit(code).str.replace(".PA", ""),
        cotation=pl.lit(cotation),
        day_variation=pl.lit(day_variation),
        quote_type=pl.lit(quote_type),
    )
    return table


def prepare_table(df_releves, df_gb):
    df_cou = df_releves.filter(pl.col("type").str.starts_with("COU"))
    df_cou = df_cou.with_columns(pl.col("titre").alias("produit"))
    df_div = df_cou.group_by("produit").agg(pl.col("montant").sum())
    df_resume = df_gb.join(df_div, on="produit", how="outer")

    df_resume = df_resume.with_columns(
        ((pl.col("cotation") - pl.col("PRU")) / pl.col("PRU") * 100)
        .round(2)
        .alias("evolution"),
        ((pl.col("cotation") - pl.col("PRU")) * pl.col("nombre")).alias("perf"),
        (
            (pl.col("cotation") - pl.col("PRU")) * pl.col("nombre") + pl.col("montant")
        ).alias("perf_div"),
    )

    df_resume = df_resume.with_columns(
        (pl.col("montant_net") + pl.col("perf")).alias("valeur")
    )

    df_resume = df_resume.with_columns(pl.col("perf_div").fill_null(pl.col("perf")))
    return df_resume


def table_global(df: pl.DataFrame):
    df = df.select(
        "code",
        "nombre",
        "valeur",
        "PRU",
        "cotation",
        "evolution",
        "perf",
        "perf_div",
        "day_variation",
    )
    df = df.to_pandas()
    df = df.sort_values(by="valeur", ascending=False)
    df_color = df.style.applymap(background_color_table, subset=["perf_div"]).format(
        {
            "valeur": "{:.2f}",
            "PRU": "{:.2f}",
            "cotation": "{:.2f}",
            "evolution": "{:.2f}",
            "perf": "{:.2f}",
            "perf_div": "{:.2f}",
            "day_variation": "{:.2f}",
        }
    )
    return st.dataframe(
        data=df_color,
        hide_index=True,
        column_order=[
            "code",
            "nombre",
            "valeur",
            "PRU",
            "cotation",
            "perf",
            "evolution",
            "perf_div",
            "day_variation",
        ],
        column_config={
            "code": "Titre 📘",
            "nombre": "Quantité 📚",
            "PRU": "PRU 🧮",
            "cotation": "Cotation 📈",
            "valeur": st.column_config.NumberColumn("Valeur 📊", format="%.2f"),
            "evolution": "Evolution 🎯",
            "perf_div": "Performance 🎢",
            "perf": "+/- Value 🔎",
            "day_variation": "Δ/j 📅",
        },
    )
