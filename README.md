# Análise do Mercado de Trabalho no Nordeste — Novo CAGED

Este projeto tem como objetivo analisar a dinâmica do mercado de trabalho formal na região Nordeste do Brasil, utilizando os microdados oficiais do **Novo CAGED**, por meio de um pipeline completo de dados (ETL) e visualização analítica em **Power BI**.

O projeto percorre todas as etapas de um fluxo de dados: **extração, transformação, carga, modelagem e visualização**, transformando dados brutos em **informação estratégica para apoio à tomada de decisão**.

---

## Objetivo

Construir uma solução analítica que permita:

* Monitorar admissões, desligamentos e saldo de empregos;
* Analisar a evolução temporal do mercado de trabalho;
* Comparar o desempenho dos estados do Nordeste;
* Investigar padrões setoriais e demográficos;
* Gerar insights estratégicos sobre o comportamento do emprego formal na região.

---

## Arquitetura da Solução

O pipeline de dados foi estruturado em três principais camadas:

```
Python (ETL) → PostgreSQL (Armazenamento) → Power BI (Visualiação)
```

### Fluxo Geral:

1. Extração dos microdados do Novo CAGED via FTP;
2. Tratamento, limpeza e padronização em Python;
3. Armazenamento em banco de dados PostgreSQL;
4. Modelagem analítica;
5. Visualização interativa no Power BI.

---

## Tecnologias Utilizadas

* **Python** — Extração (Via FTP), tratamento e automação do pipeline (ETL)
* **Pandas & NumPy** — Manipulação e transformação de dados
* **SQL & PostgreSQL** — Armazenamento e organização da base
* **Power BI** — Modelagem analítica, visualização e dashboard interativo

---

## Estrutura do Pipeline ETL

O pipeline em Python foi dividido em **dois scripts principais**, garantindo escalabilidade e facilidade de atualização.

### Script 1 — CARGA INICIAL (`etl_carga_inicial.py`)

Responsável por realizar a **primeira carga** da base de dados.

**Etapas:**

* Conexão com o FTP do Ministério do Trabalho;
* Download do arquivo CAGEDMOV;
* Descompactação automática;
* Leitura e tratamento dos dados;
* Padronização de colunas;
* Criação de variáveis derivadas:

  * Setor econômico
  * Faixa etária
* Filtragem apenas da região Nordeste;
* Envio da base tratada para o PostgreSQL.

**Uso:**

```bash
python etl_carga_inicial.py
```

Este script deve ser executado **apenas na primeira carga do projeto**.

---

### Script 2 — ATUALIZAÇÃO MENSAL (`etl_atualizações.py`)

Responsável por **atualizar automaticamente** a base com novas competências mensais, lidando com três tipos de arquivos do CAGED (CAGEDMOV, CAGEDFOR e CAGEDEXC).

**Etapas:**

* Download dos arquivos:

  * CAGEDMOV (movimentações)
  * CAGEDFOR (movimentações fora do prazo)
  * CAGEDEXC (exclusão das movimentações)
* Tratamento e padronização dos dados;
* Identificação e remoção de registros duplicados (código SQL);
* Atualização incremental da base PostgreSQL;
* Garantia de integridade dos dados.

**Uso:**

```bash
python etl_atualizações.py
```

Este script deve ser executado **sempre que houver nova competência disponível** para garantir integridade das informações e êxito na atualização da base de dados.

---

## Banco de Dados — PostgreSQL

O PostgreSQL é utilizado como **camada central de armazenamento**, permitindo:

* Persistência dos dados históricos;
* Atualizações incrementais;
* Organização estruturada da base;
* Integração direta com o Power BI.

**Tabela principal:**

* `caged_movimentacao`

## Exclusão das informações (CAGEDEXC)

Um dos principais desafios do Novo CAGED é processar o arquivo de "Desconsiderados" sem possuir idetificadores únicos, como o CPF, na base pública.
Nesse sentido, para ultrapassar essa barreira, foi preciso utilizar uma CTE (Common Table Expression) no postgreSQL que possibilitasse a numeração das ocorrências duplicadas na tabela principal e a numeração das solicitações de exclusões contidas no arquivo **CAGEDEXC** para que realizasse um 'match' exato entre as ocorrências e deletasse apenas a quantidade solicitada, preservando os dados legítimos. Essa etapa foi impretentada ao `etl_atualizações.py`

### Script:

```bash
sql excluir_cagedexc.sql
```

---

## Dashboard — Power BI

O dashboard foi desenvolvido para transformar os dados tratados em **insights estratégicos**, permitindo uma análise clara, dinâmica e interativa do mercado de trabalho formal no Nordeste.

### Principais Indicadores:

* Total de admissões
* Total de desligamentos
* Saldo líquido de empregos
* Evolução mensal do saldo
* Ranking dos estados
* Distribuição setorial
* Análise demográfica

### Funcionalidades Analíticas:

* Filtros dinâmicos por período, estado e CBO
* Drill-down setorial (setor → subsetor)
* Análise temporal
* Segmentações demográficas

🔗 **Dashboard Interativo:**
[LINK](https://app.powerbi.com/view?r=eyJrIjoiZjc3MGYwNTUtOTkwNy00ZGNjLWEwZjEtNWI1ODVkMDNkNWRkIiwidCI6ImUyZjc3ZDAwLTAxNjMtNGNmNi05MmIwLTQ4NGJhZmY5ZGY3ZCJ9)

---

## Estrutura do Repositório

```
caged-nordeste-analytics/
│
├── etl/
│   ├── etl_carga_inicial.py
│   └── etl_atualizações.py
│
├── sql/
│   └── scripts.sql
│
├── powerbi/
│   └── dashboard.pbix
│
├── auxiliary/
│   └── muni.xlsx
│   └── cbo.xlsx
|
├── README.md
└── requirements.txt
```

---

##  Como Executar o Projeto

### Clonar o repositório

```bash
git clone https://github.com/seu-usuario/caged-nordeste-analytics.git
```

### Instalar dependências

```bash
pip install -r requirements.txt
```
### Criação do Banco de dados

```bash
sql CREATE DATABASE projeto_caged;
```

### Executar carga inicial

```bash
python etl_carga_inicial.py
```

### Atualizações mensais

```bash
python etl_atualizações.py
```

---

## Observações

* Os dados são provenientes de fonte pública oficial (Novo CAGED).
* O projeto foi desenvolvido para fins educacionais, portfólio e aprimoramento técnico.
* O pipeline foi estruturado com foco em boas práticas análise de dados.
* Os comandos referente ao SQL foram implementados no `etl_atualizações.py` utilizando-se da função `text`.
* O caminho da FTP sempre deve ser alterado para a comptência que deseja realizar o download.
* Os ar


---

## Autor

**Wellington Mariano Pedro**

Estudante de Ciências Econômicas — UFPE

Foco em Data Analytics e Business Intelligence.

[Linkedin](https://www.linkedin.com/in/wellington-mariano-985a39231/)

---

## Considerações Finais

Este projeto demonstra a construção de uma solução analítica completa, indo desde a ingestão automatizada dos dados até a geração de insights estratégicos por meio de dashboards interativos.

Se este projeto foi útil, sinta-se à vontade para deixar uma ⭐ no repositório.
