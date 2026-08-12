from pyspark.sql import functions as F


def validar_reconciliacao(entrada_df, trusted_df, quarantine_df):
    entrada_count = entrada_df.count()
    trusted_count = trusted_df.count()
    quarantine_count = quarantine_df.count()

    if trusted_count + quarantine_count != entrada_count:
        raise ValueError(
            f"Falha na reconciliação: entrada={entrada_count}, "
            f"trusted={trusted_count}, quarantine={quarantine_count}"
        )

    print("Validação de reconciliação aprovada!")


def validar_customer_id_nulo(trusted_df):
    qtd_nulos = (
        trusted_df
        .filter(F.col("customer_id").isNull())
        .count()
    )

    if qtd_nulos > 0:
        raise ValueError(
            f"Falha de qualidade: existem {qtd_nulos} "
            f"customer_id nulos na camada trusted"
        )

    print("Validação de customer_id nulo aprovada!")


def validar_duplicidade_sale_id(trusted_df):
    qtd_duplicados = (
        trusted_df
        .groupBy("sale_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if qtd_duplicados > 0:
        raise ValueError(
            f"Falha de qualidade: existem {qtd_duplicados} "
            f"sale_id duplicados na camada trusted"
        )

    print("Validação de duplicidade aprovada!")