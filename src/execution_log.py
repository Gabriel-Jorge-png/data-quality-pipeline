import json
import os
from datetime import datetime

import boto3


def registrar_execucao(metricas, status, logs_path):
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

    conteudo = json.dumps(
        log_execucao,
        indent=4,
        ensure_ascii=False,
    )

    nome_arquivo = agora.strftime(
        "execution_%Y%m%d_%H%M%S.json"
    )

    if logs_path.startswith("s3://"):
        caminho = logs_path.replace("s3://", "", 1)
        bucket, prefixo = caminho.split("/", 1)

        chave = f"{prefixo.rstrip('/')}/{nome_arquivo}"

        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=bucket,
            Key=chave,
            Body=conteudo.encode("utf-8"),
            ContentType="application/json",
        )

        print(f"Log salvo em: s3://{bucket}/{chave}")

    else:
        os.makedirs(logs_path, exist_ok=True)

        caminho_arquivo = os.path.join(
            logs_path,
            nome_arquivo,
        )

        with open(
            caminho_arquivo,
            "w",
            encoding="utf-8",
        ) as arquivo:
            arquivo.write(conteudo)

        print(f"Log salvo em: {caminho_arquivo}")

def definir_status(metricas, limite_warning=5.0, limite_critico=30.0):
    taxa = metricas["taxa_rejeicao"]

    if taxa > limite_critico:
        return "CRITICO"

    if taxa > limite_warning:
        return "WARNING"

    return "SUCESSO"