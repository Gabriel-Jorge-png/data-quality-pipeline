# Data Quality Pipeline com PySpark

Pipeline de engenharia de dados desenvolvido para simular um cenário real de ingestão, validação, tratamento e monitoramento de qualidade de dados.

O projeto processa dados de **vendas e clientes**, identifica problemas de qualidade, separa registros inválidos em uma área de quarentena e disponibiliza apenas dados validados na camada `trusted`.

Além do tratamento dos dados, o pipeline implementa métricas, validações automáticas, alertas e logs de execução.

## Objetivo

O objetivo do projeto é demonstrar práticas utilizadas em pipelines de dados, incluindo:

* processamento de dados com PySpark;
* validação de qualidade;
* tratamento de valores nulos;
* identificação e tratamento de duplicidades;
* deduplicação utilizando dados mais recentes;
* quarentena de registros inválidos;
* enriquecimento de dados através de joins;
* validação de integridade antes e depois das transformações;
* métricas de execução;
* alertas de qualidade;
* monitoramento de performance;
* logs estruturados das execuções;
* organização modular do código.

## Cenário

O pipeline recebe dois conjuntos principais de dados:

### Vendas

Contém informações como:

* `sale_id`;
* `customer_id`;
* produto;
* categoria;
* quantidade;
* valor unitário;
* valor total;
* forma de pagamento;
* status;
* data da venda.

### Clientes

Contém:

* `customer_id`;
* nome;
* e-mail;
* cidade;
* estado;
* `updated_at`.

Os datasets possuem problemas propositalmente inseridos para simular situações encontradas em pipelines reais.

Entre eles:

* vendas sem `customer_id`;
* clientes duplicados;
* diferentes versões do mesmo cliente;
* valores totais negativos.

## Fluxo do pipeline

O processamento segue, de forma simplificada, este fluxo:

```text
CSV de vendas + CSV de clientes
              |
              v
        Leitura com Spark
              |
              v
      Regras de Data Quality
              |
       +------+------+
       |             |
       v             v
    Válidos       Inválidos
       |             |
       v             v
Deduplicação     Quarantine
de clientes
       |
       v
Join vendas/clientes
       |
       v
     Trusted
       |
       v
Validações finais
       |
       v
Métricas + Alertas + Logs
```

## Regras de qualidade

### Customer ID nulo

Vendas que não possuem `customer_id` são consideradas inválidas.

Esses registros não interrompem necessariamente todo o processamento. Eles são direcionados para a camada de quarentena:

```text
motivo_quarentena = "customer_id nulo"
```

Os registros válidos continuam sendo processados.

### Valor total negativo

Vendas com:

```text
valor_total < 0
```

também são enviadas para quarentena com:

```text
motivo_quarentena = "valor_total negativo"
```

A arquitetura permite adicionar novas regras de qualidade posteriormente.

## Deduplicação de clientes

A base de clientes pode conter mais de uma versão do mesmo `customer_id`.

Para evitar multiplicação de registros durante o join, os clientes são deduplicados antes do enriquecimento das vendas.

O pipeline utiliza uma janela PySpark particionada por:

```text
customer_id
```

e ordenada por:

```text
updated_at DESC
```

Dessa forma, apenas o registro mais recente de cada cliente é mantido.

No dataset utilizado no projeto:

```text
35 registros de clientes
        ↓
32 clientes únicos
```

## Join e prevenção de duplicidades

Depois da deduplicação, as vendas válidas são enriquecidas com informações dos clientes.

Uma validação compara a quantidade de vendas antes e depois do join.

Exemplo observado:

```text
Vendas antes do join: 33
Vendas depois do join: 33
Vendas duplicadas após o join: 0
```

Isso garante que o join não esteja multiplicando registros inesperadamente.

## Trusted e Quarantine

O pipeline utiliza duas saídas.

### Trusted

Contém somente os registros aprovados pelas regras de qualidade e enriquecidos com os dados dos clientes.

Os dados são persistidos em formato Parquet:

```text
data/trusted/
```

### Quarantine

Contém registros rejeitados pelas regras de qualidade:

```text
data/quarantine/
```

Cada registro possui a coluna:

```text
motivo_quarentena
```

Isso permite identificar por que determinado dado não foi enviado para a camada confiável.

## Reconciliação dos registros

Depois da gravação, os arquivos Parquet são lidos novamente e validados.

O pipeline verifica a regra:

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

Essa validação ajuda a detectar perda ou criação inesperada de registros durante o processamento.

## Validações automáticas

O pipeline possui validações que podem interromper a execução quando uma regra crítica é violada.

Atualmente são verificadas:

* reconciliação entre entrada, trusted e quarantine;
* ausência de `customer_id` nulo na camada trusted;
* ausência de `sale_id` duplicado na camada trusted.

Quando uma regra crítica falha, uma exceção é gerada.

## Métricas

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

Essas informações permitem identificar rapidamente quais problemas de qualidade estão ocorrendo com maior frequência.

## Alertas de qualidade

O pipeline possui diferentes níveis de comportamento de acordo com a taxa de rejeição.

Exemplo de configuração:

```text
Taxa <= 5%
    Normal

Taxa > 5% e <= 30%
    WARNING

Taxa > 30%
    CRÍTICO
```

Um `WARNING` permite que os registros válidos continuem sendo processados.

Uma situação crítica gera uma exceção e interrompe a execução.

## Monitoramento de performance

O tempo total de execução também é registrado.

Exemplo:

```text
duracao_segundos: 11.22
```

O pipeline pode emitir um warning quando a duração ultrapassa o limite configurado.

Isso permite detectar degradações de performance ao longo do tempo.

## Logs de execução

Cada execução gera um arquivo JSON dentro de:

```text
logs/
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

Isso cria um histórico simples das execuções e facilita a investigação de problemas.

## Organização do projeto

```text
data-quality-pipeline/
│
├── data/
│   ├── vendas.csv
│   ├── clientes.csv
│   ├── trusted/
│   └── quarantine/
│
├── logs/
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
└── terraform/
```

### Responsabilidade dos módulos

**`glue_job.py`**

Orquestra a execução do pipeline.

**`quality.py`**

Contém as regras responsáveis pela classificação dos registros entre válidos e quarentena.

**`transformations.py`**

Contém transformações como deduplicação de clientes e enriquecimento das vendas.

**`validations.py`**

Contém as validações críticas de integridade e qualidade.

**`metrics.py`**

Calcula métricas e verifica limites de qualidade e performance.

**`execution_log.py`**

Registra informações sobre cada execução do pipeline.

**`io_utils.py`**

Centraliza operações de leitura e escrita das camadas processadas.

## Tecnologias utilizadas

* Python
* PySpark
* Apache Spark
* Parquet
* Git
* WSL 2
* Linux
* Terraform — infraestrutura será adicionada na evolução AWS do projeto.

## Execução local

O projeto foi desenvolvido utilizando WSL 2.

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

Ao final da execução, o pipeline gera:

```text
data/trusted/
data/quarantine/
logs/
```

## Próximas evoluções

O projeto será expandido para simular uma arquitetura de dados em AWS.

Entre as próximas etapas planejadas estão:

* provisionamento de infraestrutura com Terraform;
* armazenamento no Amazon S3;
* execução utilizando AWS Glue;
* catálogo de dados;
* monitoramento com CloudWatch;
* alertas;
* IAM seguindo o princípio de menor privilégio;
* testes automatizados;
* parametrização das regras de qualidade;
* CI/CD.

## O que este projeto demonstra

Mais do que executar transformações com Spark, este projeto busca demonstrar preocupação com a confiabilidade operacional de um pipeline de dados.

O fluxo foi desenvolvido para responder perguntas como:

* Quantos registros entraram?
* Quantos foram processados?
* Quantos foram rejeitados?
* Por que foram rejeitados?
* Algum registro foi perdido?
* O join criou duplicidades?
* Dados inválidos chegaram à camada trusted?
* A qualidade dos dados piorou?
* A execução ficou mais lenta?
* O pipeline deve continuar ou interromper?

Esses controles tornam o pipeline mais observável, auditável e seguro para evoluir para um ambiente de produção.
