import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from quality import classificar_vendas, separar_vendas

from transformations import (
    deduplicar_clientes,
    enriquecer_vendas_com_clientes,
)

from validations import (
    validar_reconciliacao,
    validar_customer_id_nulo,
    validar_duplicidade_sale_id,
)

from io_utils import (
    gravar_dados,
    ler_dados_gravados,
)

from metrics import (
    calcular_metricas,
    exibir_metricas,
    exibir_rejeicoes_por_motivo,
    verificar_taxa_rejeicao,
    verificar_duracao_pipeline,
)

from execution_log import registrar_execucao, definir_status

inicio_execucao = time.time()

parser = argparse.ArgumentParser()

parser.add_argument(
    "--vendas-path",
    default="data/vendas.csv"
)

parser.add_argument(
    "--clientes-path",
    default="data/clientes.csv"
)

parser.add_argument(
    "--trusted-path",
    default="data/trusted"
)

parser.add_argument(
    "--quarantine-path",
    default="data/quarantine"
)

parser.add_argument(
    "--logs-path",
    default="logs"
)

args, _ = parser.parse_known_args()

# Cria a sessão Spark
spark = (
    SparkSession.builder
    .appName("data-quality-pipeline")
    .getOrCreate()
)

# Lê os arquivos da camada de entrada
vendas_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(args.vendas_path)
)

clientes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(args.clientes_path)
)

# Quantidade de registros
print(f"Quantidade de vendas: {vendas_df.count()}")
print(f"Quantidade de clientes: {clientes_df.count()}")

# Schemas identificados pelo Spark
print("Schema de vendas:")
vendas_df.printSchema()

print("Schema de clientes:")
clientes_df.printSchema()

# Visualiza alguns registros
print("Amostra de vendas:")
vendas_df.show(10, truncate=False)

print("Amostra de clientes:")
clientes_df.show(10, truncate=False)

# Quantidade de vendas com customer_id nulo
qtd_nulos = vendas_df.filter(
    F.col("customer_id").isNull()
).count()

print(f"Vendas com customer_id nulo: {qtd_nulos}")

# Identifica customer_id duplicado na tabela de clientes
clientes_duplicados = (
    clientes_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Clientes duplicados:")
clientes_duplicados.show()

# Separa vendas válidas e inválidas
vendas_classificadas_df = classificar_vendas(vendas_df)

vendas_validas_df, vendas_quarentena_df = separar_vendas(
    vendas_classificadas_df
)

print(f"Quantidade de vendas: {vendas_df.count()}")
print(f"Vendas válidas: {vendas_validas_df.count()}")
print(f"Vendas em quarentena: {vendas_quarentena_df.count()}")

print("Registros enviados para quarentena:")
vendas_quarentena_df.show(truncate=False)

# Mantém somente o registro mais recente de cada cliente
clientes_unicos_df = deduplicar_clientes(clientes_df)

print(f"Clientes após deduplicação: {clientes_unicos_df.count()}")

clientes_unicos_df.orderBy("customer_id").show(40, truncate=False)

# Join das vendas válidas com clientes já deduplicados
vendas_tratadas_df = enriquecer_vendas_com_clientes(
    vendas_validas_df,
    clientes_unicos_df
)

print(f"Vendas antes do join: {vendas_validas_df.count()}")
print(f"Vendas depois do join: {vendas_tratadas_df.count()}")

# Verifica se alguma venda ficou duplicada
vendas_duplicadas_df = (
    vendas_tratadas_df
    .groupBy("sale_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Vendas duplicadas após o join:")
vendas_duplicadas_df.show()

# Grava a camada tratada e os registros inválidos em quarentena
gravar_dados(
    vendas_tratadas_df,
    vendas_quarentena_df,
    args.trusted_path,
    args.quarantine_path,
)

# Relê os dados gravados para validar a saída real
trusted_check_df, quarantine_check_df = ler_dados_gravados(
    spark,
    args.trusted_path,
    args.quarantine_path,
)

# Valida se nenhum registro foi perdido ou criado
validar_reconciliacao(
    vendas_df,
    trusted_check_df,
    quarantine_check_df
)

validar_customer_id_nulo(trusted_check_df)

validar_duplicidade_sale_id(trusted_check_df)

metricas = calcular_metricas(
    vendas_df,
    trusted_check_df,
    quarantine_check_df,
)

exibir_metricas(metricas)
exibir_rejeicoes_por_motivo(quarantine_check_df)
verificar_taxa_rejeicao(metricas)

fim_execucao = time.time()
duracao_segundos = round(fim_execucao - inicio_execucao, 2)

metricas["duracao_segundos"] = duracao_segundos
verificar_duracao_pipeline(metricas)

status_execucao = definir_status(metricas)

registrar_execucao(
    metricas,
    status=status_execucao,
    logs_path=args.logs_path,
)

spark.stop()