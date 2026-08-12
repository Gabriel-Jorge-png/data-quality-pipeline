from pyspark.sql import functions as F

def classificar_vendas(vendas_df):
    return (
        vendas_df
        .withColumn(
            "motivo_quarentena",
            F.when(
                F.col("customer_id").isNull(),
                F.lit("customer_id nulo")
            )
            .when(
                F.col("valor_total") < 0,
                F.lit("valor_total negativo")
            )
            .otherwise(F.lit(None))
        )
    )

def separar_vendas(vendas_classificadas_df):
    vendas_quarentena_df = (
        vendas_classificadas_df
        .filter(F.col("motivo_quarentena").isNotNull())
    )

    vendas_validas_df = (
        vendas_classificadas_df
        .filter(F.col("motivo_quarentena").isNull())
        .drop("motivo_quarentena")
    )

    return vendas_validas_df, vendas_quarentena_df