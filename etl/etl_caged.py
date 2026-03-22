# bibliotecas
import ftplib
import os
import py7zr
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Váriaveis de controle do ETL
ano_atual = '2025'
competencia = '12'
filtro_ano = ['2025']
senha_BD = '34232118'

# engine
print('1/x - configurando o engine para a conexção com o SGBD')

engine = f'postgresql://postgres:{senha_BD}@localhost/projeto_caged'

# FTP
print('2/x - Acessando a FTP do MTE')

try:
    ftp = ftplib.FTP('ftp.mtps.gov.br')
    ftp.encoding = 'latin-1'
    ftp.login()
    ftp.cwd(f'/pdet/microdados/Novo CAGED/{ano_atual}/{ano_atual}{competencia}/')
except Exception as e:
    print(f'Ao tentar entrar na FTP, ocorreu um erro do tipo: {e}')
    exit()

# Baixando arquivos
print('3/x - Baixando os arquivos CAGED')

arquivos_caged = ftp.nlst()

for arquivo in arquivos_caged:
    try:
        print(f'    * Baixando o {arquivo}')
        with open(arquivo,'wb') as pasta:
            ftp.retrbinary(f'RETR {arquivo}',pasta.write)
    except Exception as e:
        print(f'Erro do tipo: {e} ao baixar o {arquivo}')
        exit()

# Descompactando arquivos
print('4/x - Descompactando os arquivos baixados')

for arquivo in arquivos_caged:
    try:
        with py7zr.SevenZipFile(arquivo, mode='r') as arq:
            arq.extractall()
            print(f'    * {arquivo} descompactado!')
    except Exception as e:
        print('Ao tentar descompactar o arquivo {arquivo}, ocorreu um erro do tipo: {e}')

# Carregando os dados
print('5/x - Carregando os dados')

arquivos_caged_txt = [i.replace('.7z','.txt') for i in arquivos_caged]
try:
    caged_mov = pd.read_csv(arquivos_caged_txt[2], sep = ';', encoding = 'utf-8-sig', dtype = str, decimal = ',')
    caged_for = pd.read_csv(arquivos_caged_txt[1], sep = ';', encoding = 'utf-8-sig', dtype = str, decimal = ',')
    caged_exc = pd.read_csv(arquivos_caged_txt[0], sep = ';', encoding = 'utf-8-sig', dtype = str, decimal = ',')
except Exception as e:
    print('Erro do tipo: {e} ao carregar os arquivos')
    exit()

# Realizando o Tratamento
print('6/x - Tratando os arquivos')

# filtrando região do Nordeste
caged_mov = caged_mov[caged_mov['região'].isin('2')]
caged_for = caged_for[caged_for['região'].isin('2')]
caged_exc = caged_exc[caged_exc['região'].isin('2')]

# Criando variáveis mes, ano e data_competencia
dados = [caged_mov,caged_for,caged_exc]

for dado in dados:
    try:
        dado['ano'] = dado['competênciamov'].str[:4]
        dado['mes'] = dado['competênciamov'].str[4:]
        dado['data_competencia'] = pd.to_datetime(dado['ano'] + '-' + dado['mes'] + '-01')
    except Exception as e:
        print(f'Ao tentar a criação das variáveis ocorreu um erro do tipo: {e}')
        exit()

# Filtrando os arquivos caged_for e caged_exc
caged_for = caged_for[caged_for['ano'].isin(filtro_ano)].copy()
caged_exc = caged_exc[caged_exc['ano'].isin(filtro_ano)].copy()

# Definindo as colunas de interesse
colunas_interesse = ['data_competencia','ano','mes','região','uf','município','seção','saldomovimentação','graudeinstrução','idade','raçacor','sexo','cbo2002ocupação']

dados = [caged_mov,caged_for, caged_exc]
for dado in dados:
    try:
        colunas_drop = [col for col in dado.columns if col not in colunas_interesse]
        dado.drop(columns = colunas_drop, inplace = True)
    except Exception as e:
        print(f'Ao realizar a seleção de colunas do {dado}, ocorreu um erro do tipo: {e}')
        exit()

# Definindo o tipo das variáveis
colunas_numericas = ['região','uf','município','saldomovimentação','graudeinstrução','raçacor','sexo','idade','ano','mes']

for dado in dados:
    for col in colunas_numericas:
        dado[col] = pd.to_numeric(dado[col], errors = 'coerce').fillna(0).astype(int)

# Criando variável faixa_etaria
for dado in dados:
        dado['faixa_etaria'] = np.select(
        [dado['idade'].isin([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]),
        dado['idade'].isin([18,19,20,21,22,23,24]),
        dado['idade'].isin([25,26,27,28,29]),
        dado['idade'].isin([30,31,32,33,34,35,36,37,38,39]),
        dado['idade'].isin([40,41,42,43,44,45,46,47,48,49]),
        dado['idade'].isin([50,51,52,53,54,55,56,57,58,59,60,61,62,63,64]),
        dado['idade'] >= 65],
        ['Até 17 anos','18 a 24 anos','25 a 29 anos','30 a 39 anos','40 a 49 anos','50 a 64 anos','65 anos ou mais'],
        default='Desconhecidos'
    )

# Renomeando as colunas
rename_coluna = {
    'município': 'municipio','seção': 'secao','região': 'regiao','saldomovimentação': 'saldo_movimentacao','graudeinstrução': 'instrucao','raçacor': 'raca_cor','cbo2002ocupação': 'cbo'}

for dado in dados:
    dado.rename(columns = rename_coluna, inplace = True)

# Unindo os arquivos Caged_for e Caged_mov
try:
    caged_join = pd.concat([caged_mov,caged_for], ignore_index = True)
except Exception as e:
    print(f'Ao unir os bancos de dados ocorreu um erro do tipo: {e}')
    exit()

# Conectando com o postgreSQL
print('7/x - Realizando a conexão com o postgreSQL')
try:
   engine =  create_engine(engine)
except Exception as e:
   print('Ao tentar se conetar ao PostgreSQL, ocorreu um erro do tipo: {}'.format(e))
   exit()

# Processando as solicitações do arquivo caged_exc
print('8/x - Processando arquivo caged_exc')
caged_exc.to_sql('tab_exc_temp', engine, if_exists = 'replace', index = False)

try:
    query = text("""
        WITH pares_para_apagar AS (
             SELECT f.id_registro
             FROM (
                 SELECT
                     id_registro, regiao, uf, municipio, secao, saldo_movimentacao, cbo, instrucao, idade, raca_cor, sexo, ano, mes, data_competencia, faixa_etaria,
                     ROW_NUMBER() OVER (
                         PARTITION BY regiao, uf, municipio, secao, saldo_movimentacao, cbo, instrucao, idade, raca_cor, sexo, ano, mes, data_competencia, faixa_etaria
                         ORDER BY id_registro
                     ) AS rn_main
                 FROM ft_caged
             ) f
             JOIN (
                 SELECT
                     regiao, uf, municipio, secao, saldo_movimentacao, cbo, instrucao, idade, raca_cor, sexo, ano, mes, data_competencia, faixa_etaria,
                     ROW_NUMBER() OVER (
                         PARTITION BY regiao, uf, municipio, secao, saldo_movimentacao, cbo, instrucao, idade, raca_cor, sexo, ano, mes, data_competencia, faixa_etaria
                         ORDER BY 1
                     ) AS rn_exc
                 FROM tab_exc_temp
             ) d
             ON  f.regiao = d.regiao 
             AND f.uf = d.uf
             AND f.municipio = d.municipio
             AND f.secao = d.secao
             AND f.saldo_movimentacao = d.saldo_movimentacao
             AND f.cbo = d.cbo
             AND f.instrucao = d.instrucao
             AND f.idade = d.idade
             AND f.raca_cor = d.raca_cor
             AND f.sexo = d.sexo
             AND f.ano = d.ano
             AND f.mes = d.mes
             AND f.data_competencia = d.data_competencia
             AND f.faixa_etaria = d.faixa_etaria
             AND f.rn_main = d.rn_exc
        )
        DELETE FROM ft_caged
        WHERE id_registro IN (SELECT id_registro FROM pares_para_apagar);""")
    
    with engine.begin() as conn:
        resultado = conn.execute(query)
        conn.execute(text("DROP TABLE tab_exc_temp"))
except Exception as e:
    print(f'Ao tentar processar o caged_exc ocorreu um erro do tipo: {e}')

# Enviando para o banco as informações do caged_join
print('9/x - Atualizando o banco de dados')

try:
   caged_join.to_sql('ft_caged', engine, if_exists='append', index=False, chunksize=10000)
except Exception as e:
   print("ao tentar enviar o bando de dados MOV e FOR, ocorreu um erro do tipo {}".format(e))


# Apagando os arquivos baixados
print('10/x - Limpando a pasta')

arquivos_apagar = [arquivos_caged,arquivos_caged_txt]

for var in arquivos_apagar:
    for arquivo in var:
        if os.path.exists(arquivo):
            os.remove(arquivo)

print('Atualização do Banco de dados realizada com sucesso!')