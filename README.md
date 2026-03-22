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
FTP (Novo CAGED)
     ↓
Python ETL (Pandas, SQLAlchemy)
     ↓
PostgreSQL (Data Warehouse - Star schema)
     ↓
Power BI (Dashboard)
```

### Fluxo Geral:

1. Extração dos microdados do Novo CAGED via FTP;
2. Tratamento, limpeza e padronização em Python;
3. Armazenamento em banco de dados PostgreSQL modelado em Star Schema;
4. Visualização interativa no Power BI.

---

## Ferramentas Utilizadas

* **Python** — Extração (Via FTP), tratamento e automação do pipeline (ETL)
* **Pandas & NumPy** — Manipulação e transformação de dados
* **SQLAlchemy** — Conexão e interação com o banco de dados
* **SQL & PostgreSQL** — Armazenamento, modelagem dimensional e processamento de exclusões
* **Power BI** — Modelagem analítica, visualização e dashboard interativo

---

## Estrutura do Pipeline ETL (`etl_caged.py`)

O script `etl_caged.py` implementa um pipeline completo de ETL (Extract, Transform, Load) para coleta, tratamento e armazenamento dos microdados do Novo CAGED no PostgreSQL. O processo é dividido em 6 etapas principais:

### 1. Configuração inicial

Definição das variáveis de controle do ETL:
* **ano_atual**: ano da competência a ser processada
* **competencia**: mês da competência
* **ano_filtro**: lista de anos considerados válidos para `CAGEDFOR` e `CAGEDEXC`
* **engine**: string de conexão com o PostgreSQL

### 2. Extração dos dados (Extract)

Conexão ao servidor FTP do Ministério do Trabalho:
```bash
ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/
```
São baixados três arquivos principais:
* **CAGEDMOV** - Movimentações mensais
* **CAGEDFOR** - Movimentações fora do prazo
* **CAGEDEXC** - Exclusões e ajustes

### 3. Descompactação

Os arquivos `.7z` são extraídos automaticamente utilizando a biblioteca `py7zr`, gerando arquivos `.txt` contendo os microdados.

### 4. Transformação dos dados (Transform)

Etapa central do pipeline, que inclui:
* Leitura dos arquivos `.txt` com Pandas
* Filtro geográfico: apenas registros da **região Nordeste** (`região == 2`)
* Criação de variáveis derivadas: `ano`, `mes`, `data_competencia` e `faixa_etaria`
* Filtragem temporal de `CAGEDFOR` e `CAGEDEXC` pelo `filtro_ano`
* Seleção das colunas de interesse
* Conversão de tipos numéricos
* Padronização dos nomes das colunas (remoção de acentos e caracteres especiais)
* União de `CAGEDMOV` + `CAGEDFOR` em `caged_join`

### 5. Processamento de exclusões (CAGEDEXC)

Um dos principais desafios dos microdados do Novo CAGED é processar o arquivo de exclusões sem possuir identificadores únicos como CPF na base pública. Para contornar isso, foi implementada uma **CTE (Common Table Expression)** diretamente no script Python via `SQLAlchemy`, que:

1. Carrega `CAGEDEXC` em uma tabela temporária (`tab_exc_temp`)
2. Numera as ocorrências duplicadas na tabela principal (`ft_caged`) e no arquivo de exclusões usando `ROW_NUMBER() OVER (PARTITION BY ...)`
3. Realiza o *match* exato entre registros, deletando apenas a quantidade solicitada e preservando os dados legítimos
4. Remove a tabela temporária ao final

### 6. Carga no banco de dados (Load)

Após o processamento das exclusões, os dados do `caged_join` (`CAGEDMOV` + `CAGEDFOR`) são inseridos incrementalmente na tabela principal `ft_caged` via `to_sql` com `if_exists='append'`.

### 7. Limpeza dos arquivos baixados

Ao final, os arquivos `.7z` e `.txt` baixados são removidos automaticamente do diretório local.

---

## Banco de Dados — PostgreSQL (Star Schema)

O banco de dados foi modelado seguindo o padrão **Star Schema**, com uma tabela fato central e tabelas dimensão para enriquecer as análises.

### Tabela Fato

**`ft_caged`**

| Campo | Tipo | Descrição |
|---|---|---|
| id_registro | SERIAL PK | Identificador único |
| data_competencia | DATE | Data de referência |
| ano | INTEGER | Ano da competência |
| mes | INTEGER | Mês da competência |
| regiao | INTEGER | Código da região |
| uf | INTEGER | Código da UF |
| municipio | INTEGER | Geocódigo do município |
| secao | CHAR(2) | Seção CNAE |
| saldo_movimentacao | INTEGER | Admissão (+1) ou Desligamento (-1) |
| instrucao | INTEGER | Grau de instrução (FK → dim_instrucao) |
| idade | INTEGER | Idade do trabalhador |
| faixa_etaria | VARCHAR(15) | Faixa etária derivada |
| raca_cor | INTEGER | Raça/cor (FK → dim_raca_cor) |
| sexo | INTEGER | Sexo (FK → dim_sexo) |
| cbo | VARCHAR(6) | Código da ocupação (FK → dim_cbo) |

### Tabelas Dimensão

**`dim_municipio`** — Municípios com informações territoriais
 
| Campo | Tipo |
|---|---|
| geocodigo | INTEGER PK |
| municipio | VARCHAR(200) |
| uf | VARCHAR(2) |
| Sudene | VARCHAR(3) |
| Semiárido | VARCHAR(3) |
| Nordeste | VARCHAR(3) |
| Caatinga | VARCHAR(3) |
 
**`dim_sexo`** — Descrição dos códigos de sexo
 
**`dim_raca_cor`** — Descrição dos códigos de raça/cor
 
**`dim_instrucao`** — Descrição dos graus de instrução
 
**`dim_cbo`** — Descrição das ocupações CBO
 
---

## Dashboard — Power BI

O dashboard foi desenvolvido para transformar os dados tratados em **insights estratégicos**, permitindo uma análise clara, dinâmica e interativa do mercado de trabalho formal no Nordeste.

### Visão Geral

![Visão Geral](docs/dashboard_geral.png)

### Análise Setorial

![Análise Setorial](docs/dashboard_setor.png)

### Análise Demográfica

![Análise Demográfica](docs/dashboard_demografia.png)

🔗 **Acesse o dashboard:**
[Clique aqui](https://app.powerbi.com/view?r=eyJrIjoiZjc3MGYwNTUtOTkwNy00ZGNjLWEwZjEtNWI1ODVkMDNkNWRkIiwidCI6ImUyZjc3ZDAwLTAxNjMtNGNmNi05MmIwLTQ4NGJhZmY5ZGY3ZCJ9)

---

## Estrutura do Repositório

```
pipeline-caged-nordeste/
│
├── docs/
│   ├── dashboard_geral.png
│   ├── dashboard_setor.png
│   └── dashboard_demografia.png
│
├── etl/
│   └── etl_caged.py
│
├── sql/
│   ├── create_table_caged.sql
│   ├── create_dim_municipio.sql
│   └── create_dim_sexo_instrucao_raca_cor.sql
│   └── valores_dim_sexo_instrucao_raca_cor.sql
│
├── README.md
└── requirements.txt
```

---

##  Como Executar o Projeto

### 1. Clonar o repositório
 
```bash
git clone https://github.com/Well-Mariano/pipeline-caged-nordeste.git
cd pipeline-caged-nordeste
```
 
### 2. Instalar dependências
 
```bash
pip install -r requirements.txt
```
 
### 3. Criar a tabela fato
 
```bash
psql -U postgres -d projeto_caged -f sql/create_table_caged.sql
```
 
### 4. Criar as tabelas dimensão
 
```bash
psql -U postgres -d projeto_caged -f sql/create_dim_municipio.sql
psql -U postgres -d projeto_caged -f sql/create_dim_sexo_instrucao_raca_cor.sql
```
 
### 5. Popular as dimensões
 
```bash
psql -U postgres -d projeto_caged -f sql/valores_dim_sexo_instrucao_raca_cor.sql
```

### 6. Configurar as variáveis de controle

Abra o arquivo `etl/etl_caged.py` e ajuste as variáveis no início do script:
 
```python
ano_atual = '2025'      # Ano da competência
competencia = '12'      # Mês da competência
filtro_ano = ['2025']   # Anos válidos para CAGEDFOR e CAGEDEXC
senha_BD = 'sua_senha'  # Senha do PostgreSQL
```
 
### 7. Executar o pipeline
 
```bash
python etl/etl_caged.py
```

---
## Observações

* Os dados são provenientes de fonte pública oficial ([Novo CAGED](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/microdados-rais-e-caged)).
* O projeto foi desenvolvido para fins educacionais, portfólio e aprimoramento técnico.
* A lógica de exclusão de registros (`CAGEDEXC`) está implementada diretamente no `etl_caged.py` via `SQLAlchemy` + CTE no PostgreSQL.
* As variáveis de controle no início do script devem ser atualizadas a cada nova competência.
* **Atenção:** não suba o script com a senha do banco exposta. Considere usar variáveis de ambiente.

---

## Autor

**Wellington Mariano Pedro**

Estudante de Ciências Econômicas — UFPE

Foco em Data Analytics e Business Intelligence.

Linkedin: [https://linkedin.com/in/wellington-mariano](https://www.linkedin.com/in/wellington-mariano-985a39231/)

GitHub: [https://github.com/Well-Mariano](https://github.com/Well-Mariano)

---
## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

Se este projeto foi útil, sinta-se à vontade para deixar uma ⭐ no repositório.
