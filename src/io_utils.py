def gravar_dados(
    vendas_tratadas_df,
    vendas_quarentena_df,
    trusted_path,
    quarantine_path,
):
    # Grava a camada tratada
    (
        vendas_tratadas_df
        .write
        .mode("overwrite")
        .parquet(trusted_path)
    )

    # Grava os registros inválidos
    (
        vendas_quarentena_df
        .write
        .mode("overwrite")
        .parquet(quarantine_path)
    )
    print("Dados gravados com sucesso em Parquet.")

def ler_dados_gravados(
    spark,
    trusted_path,
    quarantine_path,
):
    trusted_df = spark.read.parquet(trusted_path)
    quarantine_df = spark.read.parquet(quarantine_path)

    return trusted_df, quarantine_df