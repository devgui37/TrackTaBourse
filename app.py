import streamlit as st
import polars as pl
import subprocess
from src.app.app_tables import tab_gb_cotation, prepare_table, table_global
from src.app.app_plot import (
    graph_repartition_type,
    grap_repartition_titre,
    graph_invest_month,
    graph_portefeuille_brut,
    graph_portefeuille_variation,
    graph_invest_carte,
    graph_frais_moyen,
    graph_coupon_temps
)
from src.scrap.yf import get_list_isin, df_evolution_portefeuille
from src.app.app_function import remove_white_space, css_button, css_tabs
from src.app.app_data import dataframe_frais

df_ordres = pl.read_parquet("./data/parquet/ordres.parquet")
df_releves = pl.read_parquet("./data/parquet/releves.parquet")
infos = pl.read_parquet(".\data\parquet\infos.parquet").select("isin", "ticker")
df_frais = dataframe_frais(df_ordres)

df_coupon = df_releves.filter(pl.col("type").str.starts_with("COUPON"))
df_ordres_gb = tab_gb_cotation(df_ordres)
df_resume = prepare_table(df_releves, df_ordres_gb)

list_isin = get_list_isin()
datoum = df_evolution_portefeuille(list_isin)

total_titre = round(df_resume.select("valeur").sum().item(), 2)
total_div = (
    df_releves.filter(pl.col("type").str.starts_with("COU"))
    .select("montant")
    .sum()
    .item()
)
total_achat = df_resume.select("montant_net").sum().item()
versement = (
    df_releves.filter(pl.col("type").str.starts_with("VIR"))
    .select("montant")
    .sum()
    .item()
)
total_port = round(versement + (total_titre - total_achat) + total_div, 2)
delta_titre = round(
    (
        df_resume.select("valeur").sum().item()
        - df_resume.select("montant_net").sum().item()
    )
    / df_resume.select("montant_net").sum().item()
    * 100,
    2,
)
perf_titre = round(
    df_resume.select("valeur").sum().item()
    - df_resume.select("montant_net").sum().item(),
    2,
)

st.set_page_config(
    page_title="Bourse",
    page_icon="🔮",
    layout="centered"
)


def main():
    remove_white_space()
    css_button()
    css_tabs()
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write("## Portefeuille boursier 🧭 ")
    with col2:
        st.metric(
            label="Total Portefeuille",
            value=str(total_port) + " €",
            delta=str(round(100 * (total_port - versement) / versement, 2))
            + " % ("
            + str(round(total_port - versement, 2))
            + "€)",
        )

    tab1, tab2, tab3 , tab4, tab5= st.tabs(
        ["🔍 Composition", "👑 Performance", "🐣 Investissement", "💰 Dividende", "💸 Frais"]
    )
    with tab1:
        col1, col2 = st.columns([5, 1])
        with col1:
            st.subheader("Vue d'ensemble")
        with col2:
            if st.button("Actualisation", type="primary"):
                subprocess.call(["python", "src/scrap/trigger.py"])
        col1, col2 = st.columns([1, 1])
        with col1:
            st.metric(
                label="Titres",
                value=str(total_titre) + " €",
                delta=str(delta_titre) + " % (" + str(perf_titre) + "€)",
                delta_color="off",
            )
        with col2:
            st.metric(label="Espèces", value=str(total_port - total_titre) + " €")
        table_global(df_resume)
        st.divider()
        on = st.toggle("Graphique par type d'actif")
        if on:
            graph_repartition_type(df_resume)
        else:
            grap_repartition_titre(df_resume)

    with tab3:
        st.subheader("Temporalité des achats")
        on = st.toggle("Mois / Années")
        if on:
            graph_invest_month(df_ordres, temps="Années")
        else:
            graph_invest_month(df_ordres, temps="Mois")
        st.divider()
        graph_invest_carte(df_ordres)

    with tab2:
        st.subheader("Performance")
        graph_portefeuille_brut(datoum)
        st.divider()
        on = st.toggle("% ou €")
        if on:
            graph_portefeuille_variation(datoum, "variation_eur")
        else:
            graph_portefeuille_variation(datoum, "variation")
    with tab4:
        col1, col2 = st.columns([2,1])
        with col1:
            st.subheader("Dividendes versés")
        with col2:
            st.metric("Dividende",
                    str(round(df_coupon.select("montant").sum().item(),2)) + " €")
        graph_coupon_temps(df_releves,df_ordres, infos)
    with tab5:
        col1, col2 = st.columns([2,1])
        with col1:
            st.subheader("Prélévements des ordres")
        with col2:
            st.metric("Prélévement", 
                    str(round(df_frais.select("ponction").sum().item(),2)) + " €")
        graph_frais_moyen(df_ordres)


if __name__ == "__main__":
    main()