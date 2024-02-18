import streamlit as st
import plotly.express as px
import polars as pl
from streamlit.delta_generator import DeltaGenerator

def graph_repartition_type(df: pl.DataFrame) -> DeltaGenerator:
    fig = px.pie(
        df, values="valeur", names="quote_type", title="Répartition des titres",
        color_discrete_sequence=px.colors.qualitative.Antique
    )
    return st.plotly_chart(fig)


def grap_repartition_titre(df: pl.DataFrame) -> DeltaGenerator:
    fig = px.pie(df, values="valeur", names="code", title="Répartition des titres",
                 color_discrete_sequence=px.colors.qualitative.Prism
                 )
    return st.plotly_chart(fig)


def graph_invest_month(df_ordres: pl.DataFrame, temps: str) -> DeltaGenerator:
    df = df_ordres.group_by("mois_annee").agg(pl.col("montant_net").sum())
    df = df.with_columns(pl.col("mois_annee").str.to_date("%m/%Y"))
    if temps == "Mois":
        fig = px.bar(
            df,
            x="mois_annee",
            y="montant_net",
            title="Montant Net par Mois",
            labels={"mois_annee": "Mois", "montant_net": "Montant Net"},
            color_discrete_sequence=["orchid"]
        )

        moyenne_mois = df.select("montant_net").mean().item()
        fig.add_hline(
            y=moyenne_mois,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Moyenne: {moyenne_mois:.2f}",
        )
    else:
        df = (
            df.with_columns(pl.col("mois_annee").dt.year())
            .group_by("mois_annee")
            .agg(pl.col("montant_net").sum())
        )
        fig = px.bar(
            df,
            x="mois_annee",
            y="montant_net",
            title="Montant Net par Mois",
            labels={"mois_annee": "Années", "montant_net": "Montant Net"},
            color_discrete_sequence=["orchid"],
        )

        moyenne_mois = df.select("montant_net").mean().item()
        fig.add_hline(
            y=moyenne_mois,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Moyenne: {moyenne_mois:.2f}",
        )

    return st.plotly_chart(fig)


def graph_invest_carte(datoum: pl.DataFrame) -> DeltaGenerator:
    infos = pl.read_parquet("./data/parquet/infos.parquet")
    datoum = datoum.join(infos, on = "isin", how = "left")
    datoum = datoum.with_columns(
        pl.col("mois_annee").str.to_date("%m/%Y")
    )
    fig = px.scatter(
        data_frame=datoum,
        x="mois_annee",
        y="jours",
        color="ticker",
        size="montant_net",
        title="Carte d'investissement",
        labels={
            "mois_annee": "Mois",
            "jours": "n° jours"
            }
    )
    
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-0.5,
    xanchor="right",
    x=0.9, 
    title = ""
    ))
    return st.plotly_chart(fig)


def graph_portefeuille_brut(datoum: pl.DataFrame) -> DeltaGenerator:
    fig = px.area(datoum, x="date", y="total_valeur", title="Evolution du portefeuille",
                  labels={"date": "", "total_valeur": "Montant Total"})
    fig = fig.add_scatter(
        x=datoum["date"],
        y=datoum["total_montant_net"],
        fill="tonexty",
        mode="lines",
        name="Prix de Revient",
        line=dict(color='rgb(15, 98, 171)'),
        fillcolor='rgba(15, 98, 171, 0.4)'
    )
    
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
    ))
    return st.plotly_chart(fig)


def graph_portefeuille_variation(datoum: pl.DataFrame, type_variation: str) -> DeltaGenerator:
    fig = px.area(
        datoum, x="date", y=type_variation, title="Evolution des plus ou moins values",
        labels={"date": "", type_variation: "Variation"},
        color_discrete_sequence=["royalblue"]
    )
    return st.plotly_chart(fig)

def graph_frais_moyen(df_ordres:pl.DataFrame):
    infos = pl.read_parquet("./data/parquet/infos.parquet").select("isin", "ticker","type")
    frais = df_ordres.select("date","isin", "nombre", "montant_brut","commission","frais","montant_net")
    df_frais = frais.join(infos, on="isin")
    df_frais = df_frais.with_columns((pl.col("commission") + pl.col("frais")).alias("ponction"))
    df_frais = df_frais.with_columns((100*pl.col("ponction")/pl.col("montant_net")).alias("taux_ponction"))
    df_frais = df_frais.with_columns((100*pl.col("commission")/pl.col("montant_net")).alias("taux_commission"))
    df_frais = df_frais.with_columns((100*pl.col("frais")/pl.col("montant_net")).alias("taux_frais"))

    fig = px.scatter(df_frais, x = "montant_net", y = 'taux_commission',color='type',
        trendline="lowess", trendline_options=dict(frac=1),
        trendline_scope = 'overall',  trendline_color_override = 'orchid')
    
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1,
    xanchor="right",
    x=0.9, 
    title = ""
    ))

    return st.plotly_chart(fig)

def graph_coupon_temps(df_releves, df_ordres, infos):
    df_coupon = df_releves.filter(pl.col("type").str.starts_with("COUPON"))
    df_coupon = df_coupon.rename({"titre": "produit"})
    df_ordres_pi = df_ordres.select("produit", "isin").unique()
    df_coupon_2 = df_coupon.join(df_ordres_pi, on = "produit").join(infos, on = "isin")
    
    fig = px.bar(df_coupon_2, x = "date", y = "montant", color="ticker",
       color_discrete_sequence=px.colors.qualitative.G10)
    
    fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1,
    xanchor="right",
    x=0.9, 
    title = ""
    ))
    
    return st.plotly_chart(fig)

