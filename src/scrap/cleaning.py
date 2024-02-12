import polars as pl


def _clean_format_A(df: pl.DataFrame) -> pl.DataFrame:
    """Remplace les "," par des "." et change les types

    Args:
        df (pl.DataFrame): df brut

    Returns:
        pl.DataFrame: df clean
    """
    data = df.with_columns(
        pl.col("cours").str.replace(",", ".").cast(pl.Float64),
        pl.col("montant_brut").str.replace(",", ".").cast(pl.Float64),
        pl.col("commission").str.replace(",", ".").cast(pl.Float64),
        pl.col("frais").str.replace(",", ".").cast(pl.Float64),
        pl.col("montant_net").str.replace(",", ".").cast(pl.Float64),
        pl.col("nombre").cast(pl.Int64),
    )
    return data


def clean_format_R(df: pl.DataFrame):
    """Remplace les "," par des "." et change les types

    Args:
        df (pl.DataFrame): df brut

    Returns:
        pl.DataFrame: df clean
    """
    data = df.with_columns(
        pl.col("montant").str.replace(",", ".").cast(pl.Float64),
    )
    return data


def _clean_date(df: pl.DataFrame) -> pl.DataFrame:
    """Gère les dates

    Args:
        df (pl.DataFrame): df brut

    Returns:
        pl.DataFrame: df clean
    """
    data = df.with_columns(
        pl.col("date").str.to_datetime(
            "%d/%m/%Y", time_zone="Europe/Paris", time_unit="ns"
        ),
        jours=pl.col("date").str.slice(0, 2).cast(pl.Int64),
        mois_annee=pl.col("date").str.slice(3, 8),
    )
    return data


def _clean_cours_unit(df: pl.DataFrame) -> pl.DataFrame:
    """Calcul le cours unitaire d'achat

    Args:
        df (pl.DataFrame): dh brut

    Returns:
        pl.DataFrame: df clean
    """
    data = df.with_columns(
        cours_unit=pl.col("montant_net") / pl.col("nombre"),
    )
    return data


def _sort(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort("date")


def clean_pipeline(df: pl.DataFrame) -> pl.DataFrame:
    """Agrège toutes les fonctions de cleaning

    Args:
        df (pl.DataFrame): df brut

    Returns:
        pl.DataFrame: df clean
    """
    data = (
        df.pipe(_clean_format_A).pipe(_clean_date).pipe(_clean_cours_unit).pipe(_sort)
    )
    data = data.drop("heure")
    return data


def clean_to_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Nettoie et créer un nouveau csv nettoyé

    Args:
        df (pl.DataFrame): df brut

    Returns:
        pl.DataFrame: df clean
    """
    data = clean_pipeline(df)
    data.write_parquet(r".\data\parquet\ordres.parquet")
    return print("Succès")
