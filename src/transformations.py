from pyspark.sql import functions as F
from pyspark.sql.window import Window

def deduplicar_clientes(clientes_df):
    window_cliente = (
        Window
        .partitionBy("customer_id")
        .orderBy(F.col("updated_at").desc())
    )

    return (
        clientes_df
        .withColumn(
            "row_number",
            F.row_number().over(window_cliente)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

def enriquecer_vendas_com_clientes(vendas_validas_df, clientes_unicos_df):
    return (
        vendas_validas_df.alias("v")
        .join(
            clientes_unicos_df.alias("c"),
            F.col("v.customer_id") == F.col("c.customer_id"),
            "left"
        )
        .select(
            "v.*",
            F.col("c.nome").alias("cliente_nome"),
            F.col("c.email").alias("cliente_email"),
            F.col("c.cidade").alias("cliente_cidade"),
            F.col("c.estado").alias("cliente_estado")
        )
    )