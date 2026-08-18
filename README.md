# Data Quality Pipeline com PySpark e AWS

Pipeline de Engenharia de Dados desenvolvido para simular um cenário real de ingestão, validação, tratamento, monitoramento e disponibilização de dados utilizando **PySpark e serviços AWS**.

O projeto processa dados de vendas e clientes, identifica problemas de qualidade, separa registros inválidos em uma camada de quarentena e disponibiliza apenas dados validados na camada `trusted`.

Além do processamento dos dados, o pipeline implementa métricas, validações automáticas, alertas, logs estruturados, infraestrutura como código e execução orientada a eventos.

---

## Objetivo

O objetivo deste projeto é demonstrar práticas utilizadas em pipelines modernos de Engenharia de Dados, incluindo:

- processamento distribuído com PySpark;
- validação de qualidade de dados;
- tratamento de valores nulos;
- identificação e tratamento de duplicidades;
- deduplicação utilizando registros mais recentes;
- quarentena de registros inválidos;
- enriquecimento de dados através de joins;
- validação de integridade antes e depois das transformações;
- geração de métricas;
- alertas de qualidade;
- monitoramento de performance;
- logs estruturados das execuções;
- armazenamento em formato Parquet;
- catálogo automático de dados;
- consultas SQL sobre Data Lake;
- infraestrutura como código com Terraform;
- automação orientada a eventos na AWS.

---

## Arquitetura

O pipeline utiliza a seguinte arquitetura:

```text
CSV
 │
 ▼
Amazon S3
raw/
 │
 ▼
Amazon EventBridge
 │
 ▼
AWS Glue Workflow
 │
 ▼
Glue Trigger
 │
 ▼
AWS Glue Job
PySpark
 │
 ├───────────────┐
 ▼               ▼
trusted/     quarantine/
Parquet         Parquet
 │
 ▼
Glue Crawler
 │
 ▼
Glue Data Catalog
 │
 ▼
Amazon Athena

Logs e métricas
      │
      ▼
Amazon S3
logs/
```

### Fluxo

1. Arquivos de entrada são armazenados na camada `raw/` do Amazon S3.
2. A criação de um novo arquivo em `raw/vendas/` gera um evento.
3. O Amazon EventBridge identifica esse evento.
4. O EventBridge encaminha o evento para um AWS Glue Workflow.
5. Um Glue Trigger inicia automaticamente o Glue Job.
6. O Glue executa o pipeline PySpark.
7. As regras de Data Quality classificam os registros.
8. Registros válidos são gravados em `trusted/`.
9. Registros inválidos são enviados para `quarantine/`.
10. As saídas são armazenadas em formato Parquet.
11. Métricas e logs estruturados são armazenados em `logs/`.
12. O Glue Crawler identifica o schema dos dados tratados.
13. O Glue Data Catalog registra a tabela.
14. Os dados podem ser consultados com SQL através do Amazon Athena.

---

## Cenário dos Dados

O pipeline recebe dois conjuntos principais de dados.

### Vendas

Contém informações como:

- `sale_id`;
- `customer_id`;
- produto;
- categoria;
- quantidade;
- valor unitário;
- valor total;
- forma de pagamento;
- status;
- data da venda.

### Clientes

Contém:

- `customer_id`;
- nome;
- e-mail;
- cidade;
- estado;
- `updated_at`.

Os datasets possuem problemas propositalmente inseridos para simular situações encontradas em pipelines reais, como:

- vendas sem `customer_id`;
- clientes duplicados;
- diferentes versões do mesmo cliente;
- valores totais negativos.

---

## Regras de Data Quality

O pipeline aplica validações antes de disponibilizar os registros para análise.

### Customer ID nulo

Vendas sem `customer_id` são consideradas inválidas e enviadas para a camada de quarentena.

```text
motivo_quarentena = "customer_id nulo"
```

Os registros válidos continuam sendo processados.

### Valor total negativo

Vendas em que:

```text
valor_total < 0
```

também são enviadas para quarentena:

```text
motivo_quarentena = "valor_total negativo"
```

A arquitetura permite adicionar novas regras de qualidade posteriormente.

---

## Deduplicação de Clientes

A base de clientes pode conter mais de uma versão do mesmo `customer_id`.

Para impedir a multiplicação de registros durante o join, os clientes são deduplicados antes do enriquecimento das vendas.

O pipeline utiliza uma Window do PySpark particionada por:

```text
customer_id
```

e ordenada por:

```text
updated_at DESC
```

Dessa forma, apenas o registro mais recente de cada cliente é mantido.

No dataset utilizado:

```text
35 registros de clientes
        ↓
32 clientes únicos
```

---

## Join e Prevenção de Duplicidades

Depois da deduplicação, as vendas válidas são enriquecidas com informações dos clientes.

Uma validação compara a quantidade de vendas antes e depois do join.

Exemplo:

```text
Vendas antes do join: 33
Vendas depois do join: 33
Vendas duplicadas após o join: 0
```

Isso permite detectar multiplicação inesperada de registros causada pelo relacionamento entre os datasets.

---

## Trusted e Quarantine

O pipeline utiliza duas principais camadas de saída.

### Trusted

Contém somente registros aprovados pelas regras de qualidade e enriquecidos com os dados dos clientes.

Na AWS:

```text
s3://<bucket>/trusted/
```

Os dados são armazenados em formato **Parquet**.

### Quarantine

Contém registros rejeitados pelas regras de qualidade.

```text
s3://<bucket>/quarantine/
```

Cada registro possui a coluna:

```text
motivo_quarentena
```

Isso permite identificar exatamente por que determinado registro não chegou à camada confiável.

---

## Reconciliação dos Registros

Depois da gravação, os dados são novamente lidos e validados.

O pipeline verifica:

```text
entrada = trusted + quarantine
```

No cenário atual:

```text
41 registros recebidos
33 registros trusted
8 registros quarantine

41 = 33 + 8
```

Caso essa igualdade não seja satisfeita, o pipeline gera um erro.

Essa validação ajuda a identificar perda ou criação inesperada de registros durante o processamento.

---

## Validações Automáticas

Entre as validações implementadas estão:

- reconciliação entre entrada, Trusted e Quarantine;
- ausência de `customer_id` nulo na camada Trusted;
- ausência de `sale_id` duplicado na camada Trusted;
- validação da quantidade de registros antes e depois do join.

Quando uma regra crítica é violada, uma exceção pode interromper a execução.

---

## Métricas de Qualidade

Cada execução calcula métricas como:

```text
Registros recebidos: 41
Registros aprovados: 33
Registros rejeitados: 8
Taxa de rejeição: 19.51%
```

Também são calculadas rejeições por motivo:

```text
customer_id nulo: 7
valor_total negativo: 1
```

Essas métricas permitem identificar rapidamente quais problemas de qualidade estão ocorrendo com maior frequência.

---

## Alertas de Qualidade

O pipeline possui diferentes comportamentos de acordo com a taxa de rejeição.

Exemplo:

```text
Taxa <= 5%
    Normal

Taxa > 5% e <= 30%
    WARNING

Taxa > 30%
    CRÍTICO
```

Um `WARNING` permite que os registros válidos continuem sendo processados.

Uma situação crítica pode interromper a execução.

---

## Monitoramento de Performance

O tempo total de processamento também é registrado.

Exemplo:

```text
duracao_segundos: 11.22
```

Isso permite acompanhar possíveis degradações de performance entre execuções.

---

## Logs de Execução

Cada execução gera um log estruturado em JSON.

Na AWS, esses logs são persistidos em:

```text
s3://<bucket>/logs/
```

Exemplo:

```json
{
    "data_execucao": "2026-08-12T15:06:58.424817",
    "registros_entrada": 41,
    "registros_trusted": 33,
    "registros_quarantine": 8,
    "taxa_rejeicao": 19.51,
    "rejeicoes_por_motivo": {
        "valor_total negativo": 1,
        "customer_id nulo": 7
    },
    "status": "WARNING",
    "duracao_segundos": 11.22
}
```

Isso cria um histórico das execuções e facilita análise, rastreabilidade e investigação de problemas.

---

## Estrutura do Projeto

```text
data-quality-pipeline/
├── data/
│   ├── vendas.csv
│   └── clientes.csv
│
├── src/
│   ├── glue_job.py
│   ├── quality.py
│   ├── transformations.py
│   ├── validations.py
│   ├── metrics.py
│   ├── execution_log.py
│   └── io_utils.py
│
├── terraform/
│   └── main.tf
│
└── README.md
```

### Responsabilidade dos Módulos

**`glue_job.py`**

Ponto de entrada e orquestração do pipeline PySpark.

**`quality.py`**

Contém regras responsáveis pela classificação dos registros entre válidos e quarentena.

**`transformations.py`**

Implementa transformações como deduplicação de clientes e enriquecimento das vendas.

**`validations.py`**

Contém validações críticas de integridade e qualidade.

**`metrics.py`**

Calcula métricas e verifica limites relacionados à qualidade e performance.

**`execution_log.py`**

Gera e persiste informações estruturadas sobre cada execução.

**`io_utils.py`**

Centraliza operações de leitura e escrita das camadas processadas.

---

## Execução Local

O mesmo pipeline pode ser executado localmente com PySpark.

Ative o ambiente virtual:

```bash
source ~/data-quality-venv/bin/activate
```

Entre no projeto:

```bash
cd ~/data-quality-pipeline
```

Execute:

```bash
python src/glue_job.py
```

Por padrão, a execução local utiliza os arquivos presentes em `data/`.

Os caminhos foram parametrizados para permitir que o mesmo código seja utilizado localmente e no AWS Glue.

---

## Execução no AWS Glue

Na AWS, os caminhos são enviados ao Glue Job através de argumentos:

```text
--vendas-path
--clientes-path
--trusted-path
--quarantine-path
--logs-path
--extra-py-files
```

O script principal do Glue Job é armazenado no Amazon S3.

Os demais módulos Python são empacotados em um arquivo ZIP e carregados pelo Glue através de:

```text
--extra-py-files
```

Dessa forma, o código continua modular mesmo quando executado no ambiente gerenciado do AWS Glue.

---

## Automação Orientada a Eventos

O pipeline não depende de execução manual.

Quando um novo arquivo é enviado para:

```text
raw/vendas/
```

o fluxo automático é iniciado:

```text
S3
 ↓
EventBridge
 ↓
Glue Workflow
 ↓
Glue Trigger
 ↓
Glue Job
```

O filtro do EventBridge monitora apenas o prefixo de entrada, evitando que arquivos criados em `trusted/`, `quarantine/` ou `logs/` iniciem novas execuções do pipeline.

---

## Glue Data Catalog e Crawler

Depois que o Glue Job grava os dados tratados em `trusted/`, um Glue Crawler pode analisar os arquivos Parquet.

O Crawler:

- percorre os arquivos da camada Trusted;
- identifica o schema;
- identifica os tipos das colunas;
- registra os metadados no Glue Data Catalog.

No projeto, os dados são registrados no database:

```text
data_quality_db
```

e disponibilizados através da tabela:

```text
trusted
```

---

## Consultas com Amazon Athena

Depois de catalogados, os dados podem ser consultados diretamente no Data Lake utilizando SQL através do Amazon Athena.

Exemplo de análise por categoria:

```sql
SELECT
    categoria,
    COUNT(*) AS quantidade_vendas,
    SUM(quantidade) AS itens_vendidos,
    ROUND(SUM(valor_total), 2) AS faturamento_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio
FROM data_quality_db.trusted
GROUP BY categoria
ORDER BY faturamento_total DESC;
```

Exemplo por estado:

```sql
SELECT
    cliente_estado,
    COUNT(*) AS quantidade_vendas,
    ROUND(SUM(valor_total), 2) AS faturamento_total
FROM data_quality_db.trusted
GROUP BY cliente_estado
ORDER BY faturamento_total DESC;
```

Exemplo por forma de pagamento:

```sql
SELECT
    forma_pagamento,
    COUNT(*) AS quantidade_vendas,
    ROUND(SUM(valor_total), 2) AS faturamento_total
FROM data_quality_db.trusted
GROUP BY forma_pagamento
ORDER BY faturamento_total DESC;
```

---

## Infraestrutura como Código

A infraestrutura AWS é provisionada utilizando **Terraform**.

Entre os recursos utilizados no projeto estão:

- Amazon S3;
- S3 Public Access Block;
- IAM Roles e Policies;
- AWS Glue Job;
- Glue Data Catalog Database;
- Glue Crawler;
- Glue Workflow;
- Glue Trigger;
- Amazon EventBridge Rule;
- EventBridge Target.

O fluxo utilizado antes de alterações na infraestrutura é:

```bash
terraform fmt
terraform validate
terraform plan
terraform apply
```

O `terraform plan` é revisado antes do `apply` para identificar alterações inesperadas, principalmente substituição ou destruição de recursos.

---

## Tecnologias Utilizadas

- Python
- PySpark
- Apache Spark
- Parquet
- Amazon S3
- AWS Glue
- AWS Glue Workflow
- AWS Glue Crawler
- AWS Glue Data Catalog
- Amazon EventBridge
- Amazon Athena
- IAM
- Terraform
- AWS CLI
- Git
- GitHub
- Linux / WSL 2

---

## Resultados

O projeto implementa um pipeline capaz de:

- executar processamento PySpark localmente e na AWS;
- identificar registros inválidos antes de disponibilizá-los para análise;
- separar registros válidos e inválidos;
- registrar o motivo das rejeições;
- deduplicar registros utilizando Window Functions;
- enriquecer dados através de joins;
- detectar multiplicação inesperada durante joins;
- reconciliar registros entre entrada e saída;
- produzir métricas de qualidade;
- monitorar duração das execuções;
- gerar logs estruturados;
- armazenar dados analíticos em Parquet;
- catalogar automaticamente o schema dos dados;
- realizar consultas SQL diretamente sobre o Data Lake;
- provisionar infraestrutura através de Terraform;
- iniciar automaticamente o pipeline quando novos arquivos chegam ao S3.

---

## O que este projeto demonstra

Mais do que executar transformações com Spark, o projeto busca demonstrar preocupação com a **confiabilidade operacional de pipelines de dados**.

O fluxo foi desenvolvido para responder perguntas como:

- Quantos registros entraram?
- Quantos foram processados?
- Quantos foram rejeitados?
- Por que foram rejeitados?
- Algum registro foi perdido?
- O join criou duplicidades?
- Dados inválidos chegaram à camada Trusted?
- A qualidade dos dados piorou?
- A execução ficou mais lenta?
- O pipeline deve continuar ou interromper?

Esses controles tornam o pipeline mais observável, auditável e preparado para evolução.

---

## Próximas Evoluções

Possíveis melhorias futuras:

- testes automatizados;
- criação de `requirements.txt`;
- CI/CD;
- alarmes adicionais no CloudWatch;
- particionamento da camada Trusted para volumes maiores;
- processamento incremental;
- estratégias de backfill e reprocessamento;
- parametrização adicional das regras de qualidade;
- dashboards para acompanhamento das métricas do pipeline.