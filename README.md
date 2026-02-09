# Análise do Mercado de Trabalho no Nordeste — Novo CAGED

Este projeto tem como objetivo analisar a dinâmica do mercado de trabalho formal na região Nordeste do Brasil, utilizando os microdados oficiais do **Novo CAGED**, por meio de um pipeline completo de dados (ETL) e visualização analítica em **Power BI**.

O projeto percorre todas as etapas de um fluxo profissional de dados: **extração, transformação, carga, modelagem e visualização**, transformando dados brutos em **informação estratégica para apoio à tomada de decisão**.

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
FTP (MTE) → Python (ETL) → PostgreSQL → Power BI
```

### Fluxo Geral:

1. Extração dos microdados do Novo CAGED via FTP;
2. Tratamento, limpeza e padronização em Python;
3. Armazenamento em banco de dados PostgreSQL;
4. Modelagem analítica;
5. Visualização interativa no Power BI.

---

## Tecnologias Utilizadas

* **Python** — Extração, tratamento e automação do pipeline (ETL)
* **Pandas & NumPy** — Manipulação e transformação de dados
* **SQL & PostgreSQL** — Armazenamento e organização da base
* **Power BI** — Modelagem analítica, visualização e dashboard interativo

---

## Estrutura do Pipeline ETL

O pipeline em Python foi dividido em **dois scripts principais**, garantindo escalabilidade e facilidade de atualização.

### Script 1 — CARGA INICIAL (`CAGED_01.py`)

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
python CAGED_01.py
```

Este script deve ser executado **apenas na primeira carga do projeto**.

---

### Script 2 — ATUALIZAÇÃO MENSAL (`CAGED_02.py`)

Responsável por **atualizar automaticamente** a base com novas competências mensais, lidando com três tipos de arquivos do CAGED (CAGEDMOV, CAGEDFOR e CAGEDEXC).

**Etapas:**

* Download dos arquivos:

  * CAGEDMOV (movimentações)
  * CAGEDFOR (fora do prazo)
  * CAGEDEXC (exclusões)
* Tratamento e padronização dos dados;
* Identificação e remoção de registros duplicados (código SQL);
* Atualização incremental da base PostgreSQL;
* Garantia de integridade dos dados.

**Uso:**

```bash
python CAGED_02.py
```

Este script pode ser executado **sempre que houver nova competência disponível**.

---

## Banco de Dados — PostgreSQL

O PostgreSQL é utilizado como **camada central de armazenamento**, permitindo:

* Persistência dos dados históricos;
* Atualizações incrementais;
* Organização estruturada da base;
* Integração direta com o Power BI.

**Tabela principal:**

* `caged_movimentacao`

---

## Exclusão das informações (CAGEDEXC)

Um dos principais desafios do Novo CAGED é processar o arquivo de "Desconsiderados" sem possuir idetificadores únicos, como o CPF, na base pública.
Nesse sentido, para ultrapassar essa barreira, foi preciso utilizar uma CTE (Common Table Expression) no postgreSQL que possibilitasse a numeração das ocorrências duplicadas na tabela principal e a numeração das solicitações de exclusões contidas no arquivo CAGEDEXC para que realizasse um 'match' exato entre as ocorrências e deletasse apenas a quantidade solicitada, preservando os dados legítimos.

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

* Filtros dinâmicos por período e estado
* Drill-down setorial (setor → subsetor)
* Análise temporal
* Segmentações demográficas

🔗 **Dashboard Interativo:**
[https://app.powerbi.com/view?r=eyJrIjoiZjc3MGYwNTUtOTkwNy00ZGNjLWEwZjEtNWI1ODVkMDNkNWRkIiwidCI6ImUyZjc3ZDAwLTAxNjMtNGNmNi05MmIwLTQ4NGJhZmY5ZGY3ZCJ9&pageName=93de5419f83b82560647](https://app.powerbi.com/view?r=eyJrIjoiZjc3MGYwNTUtOTkwNy00ZGNjLWEwZjEtNWI1ODVkMDNkNWRkIiwidCI6ImUyZjc3ZDAwLTAxNjMtNGNmNi05MmIwLTQ4NGJhZmY5ZGY3ZCJ9&pageName=93de5419f83b82560647)

---

## Estrutura do Repositório

```
caged-nordeste-analytics/
│
├── etl/
│   ├── CAGED_01.py
│   └── CAGED_02.py
│
├── sql/
│   └── scripts.sql
│
├── powerbi/
│   └── dashboard.pbix
│
├── images/
│   └── dashboard.png
│
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

### Executar carga inicial

```bash
python etl/CAGED_01.py
```

### Atualizações mensais

```bash
python etl/CAGED_02.py
```

---

## Observações

* Os dados são provenientes de fonte pública oficial (Novo CAGED).
* O projeto foi desenvolvido para fins educacionais, portfólio e aprimoramento técnico.
* O pipeline foi estruturado com foco em boas práticas análise de dados.

---

## Autor

**Wellington Mariano Pedro**
Estudante de Ciências Econômicas — UFPE
Foco em Data Analytics, Business Intelligence e Engenharia de Dados

---

## Considerações Finais

Este projeto demonstra a construção de uma solução analítica completa, indo desde a ingestão automatizada dos dados até a geração de insights estratégicos por meio de dashboards interativos.

Se este projeto foi útil, sinta-se à vontade para deixar uma ⭐ no repositório.
