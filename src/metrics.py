def calcular_metricas(vendas_df, trusted_df, quarantine_df):
    entrada = vendas_df.count()
    trusted = trusted_df.count()
    quarantine = quarantine_df.count()

    taxa_rejeicao = 0

    if entrada > 0:
        taxa_rejeicao = (quarantine / entrada) * 100

    rejeicoes_por_motivo = {
        row["motivo_quarentena"]: row["count"]
        for row in (
            quarantine_df
            .groupBy("motivo_quarentena")
            .count()
            .collect()
        )
    }

    return {
        "entrada": entrada,
        "trusted": trusted,
        "quarantine": quarantine,
        "taxa_rejeicao": taxa_rejeicao,
        "rejeicoes_por_motivo": rejeicoes_por_motivo,
    }

def exibir_metricas(metricas):
    print("=== MÉTRICAS DO PIPELINE ===")
    print(f"Registros recebidos: {metricas['entrada']}")
    print(f"Registros aprovados: {metricas['trusted']}")
    print(f"Registros rejeitados: {metricas['quarantine']}")
    print(f"Taxa de rejeição: {metricas['taxa_rejeicao']:.2f}%")

def exibir_rejeicoes_por_motivo(quarantine_df):
    print("=== REJEIÇÕES POR MOTIVO ===")

    (
        quarantine_df
        .groupBy("motivo_quarentena")
        .count()
        .orderBy("count", ascending=False)
        .show(truncate=False)
    )

def verificar_taxa_rejeicao(
    metricas,
    limite_warning=5.0,
    limite_critico=30.0,
):
    taxa = metricas["taxa_rejeicao"]

    if taxa > limite_critico:
        raise ValueError(
            f"ERRO CRÍTICO: taxa de rejeição em {taxa:.2f}% "
            f"acima do limite crítico de {limite_critico:.2f}%"
        )

    if taxa > limite_warning:
        print(
            f"WARNING: taxa de rejeição em {taxa:.2f}% "
            f"acima do limite de {limite_warning:.2f}%"
        )
    else:
        print(
            f"Taxa de rejeição dentro do limite: {taxa:.2f}%"
        )

def verificar_duracao_pipeline(metricas, limite_segundos=20.0):
    duracao = metricas["duracao_segundos"]

    if duracao > limite_segundos:
        print(
            f"WARNING: duração do pipeline em {duracao:.2f}s "
            f"acima do limite de {limite_segundos:.2f}s"
        )
    else:
        print(
            f"Duração do pipeline dentro do limite: {duracao:.2f}s"
        )