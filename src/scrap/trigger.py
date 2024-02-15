from scrap_pdf_avis import re_pipeline
from cleaning import clean_to_csv
from scrap_pdf_releves import create_csv_releves
import polars as pl
from yf import parquet_info_isin

re_pipeline(dossier="./data")
data = pl.read_parquet("./data/parquet/ordres.parquet")
clean_to_csv(data)
create_csv_releves(dossier="./data")
parquet_info_isin()
