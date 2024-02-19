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
from src.scrap.yf import get_list_isin, df_evolution_portefeuille, df_creata_all_cotation
from src.app.app_function import remove_white_space, css_button, css_tabs
from src.app.app_data import (
    dataframe_frais, 
    nombre_versement, 
    nombre_delta_titre,
    nombre_performance_titre,
    nombre_total_dividende,
    nombre_total_titre,
    nombre_total_achat,
    nombre_total_portefeuille)

df_ordres = pl.read_parquet("./data/parquet/ordres.parquet")
df_releves = pl.read_parquet("./data/parquet/releves.parquet")
df_cotation = pl.read_parquet("./data/parquet/cotation.parquet")
df_infos = pl.read_parquet("./data/parquet/infos.parquet")
infos = df_infos.select("isin", "ticker")
df_frais = dataframe_frais(df_ordres)

df_coupon = df_releves.filter(pl.col("type").str.starts_with("COUPON"))
df_ordres_gb = tab_gb_cotation(df_ordres, df_cotation,df_infos)
df_resume = prepare_table(df_releves, df_ordres_gb)

list_isin = get_list_isin()
datoum = df_evolution_portefeuille(list_isin)

total_titre = nombre_total_titre(df_resume)
total_div = nombre_total_dividende(df_releves)
total_achat = nombre_total_achat(df_resume)
versement = nombre_versement(df_releves)
total_port = nombre_total_portefeuille(versement, total_titre, total_achat, total_div)
delta_titre = nombre_delta_titre(df_resume)
perf_titre = nombre_performance_titre(df_resume)

st.set_page_config(
    page_title="Bourse",
    page_icon="🔮",
    layout="centered"
)


def main():
    remove_white_space()
    css_button()
    css_tabs()
    with st.sidebar:
        st.write("# Configuration")
        st.divider()
        st.write("### Actualisation")
        if st.button("PDF Scraping", type="primary"):
                subprocess.call(["python", "src/scrap/trigger.py"])
        if st.button("MàJ Cotation", type = "primary"):
            liste_isin = get_list_isin()
            df_creata_all_cotation(liste_isin)
        st.divider()
        st.write("### Versement total")
        st.write("Facultatif : utile si un versement a été fait depuis le dernier relevé d'espèces.")
        versement = st.number_input("Versement", 
                                    value=nombre_versement(df_releves),
                                    step=100.)
        total_port = nombre_total_portefeuille(versement, total_titre, total_achat, total_div)
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write("## Portefeuille boursier 🧭 ")
    with col2:
        st.metric(
            label="Total Portefeuille",
            value=str(round(total_port,2)) + " €",
            delta=str(round(100 * (total_port - versement) 
                            / versement, 2))
            + " % ("
            + str(round(total_port - versement, 2))
            + "€)",
        )

    tab1, tab2, tab3 , tab4, tab5= st.tabs(
        ["🔍 Composition", "👑 Performance", "🐣 Investissement", "💰 Dividende", "💸 Frais"]
    )
    with tab1:
        st.subheader("Vue d'ensemble")
        col1, col2 = st.columns([1, 1])
        with col1:
            st.metric(
                label="Titres",
                value=str(total_titre) + " €",
                delta=str(delta_titre) + " % (" + str(perf_titre) + "€)",
                delta_color="off",
            )
        with col2:
            st.metric(label="Espèces", value=str(round(versement - total_achat + total_div,2)) + " €")
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