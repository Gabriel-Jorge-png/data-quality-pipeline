import json
import os
from datetime import datetime


def registrar_execucao(metricas, status):
    os.makedirs("logs", exist_ok=True)

    agora = datetime.now()

    log_execucao = {
        "data_execucao": agora.isoformat(),
        "registros_entrada": metricas["entrada"],
        "registros_trusted": metricas["trusted"],
        "registros_quarantine": metricas["quarantine"],
        "taxa_rejeicao": round(metricas["taxa_rejeicao"], 2),
        "rejeicoes_por_motivo": metricas["rejeicoes_por_motivo"],
        "status": status,
        "duracao_segundos": metricas["duracao_segundos"],
    }

    nome_arquivo = agora.strftime(
        "logs/execution_%Y%m%d_%H%M%S.json"
    )

    with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
        json.dump(
            log_execucao,
            arquivo,
            indent=4,
            ensure_ascii=False
        )

    print(f"Log salvo em: {nome_arquivo}")

def definir_status(metricas, limite_warning=5.0, limite_critico=30.0):
    taxa = metricas["taxa_rejeicao"]

    if taxa > limite_critico:
        return "CRITICO"

    if taxa > limite_warning:
        return "WARNING"

    return "SUCESSO"