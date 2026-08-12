def gravar_dados(vendas_tratadas_df, vendas_quarentena_df):
    # Grava a camada tratada
    (
        vendas_tratadas_df
        .write
        .mode("overwrite")
        .parquet("data/trusted")
    )

    # Grava os registros inválidos
    (
        vendas_quarentena_df
        .write
        .mode("overwrite")
        .parquet("data/quarantine")
    )
    print("Dados gravados com sucesso em Parquet.")

def ler_dados_gravados(spark):
    trusted_df = spark.read.parquet("data/trusted")
    quarantine_df = spark.read.parquet("data/quarantine")

    return trusted_df, quarantine_df