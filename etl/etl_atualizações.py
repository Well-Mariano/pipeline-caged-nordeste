# Bibliotecas
import ftplib
import os
import py7zr
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Definindo variáeis 
ano_atual = '2025'
competencia = '04'
ano_filtro = ['2025','2026']
engine_sql = 'postgresql://postgres:NovaSenha123@localhost/projeto_caged'

# Entrando na FTP do MTE:
print('1/6 - Entrando na FTP do MTE')
try:
   ftp = ftplib.FTP('ftp.mtps.gov.br')
   ftp.encoding = 'latin-1'
   ftp.login()
   ftp.cwd('/pdet/microdados/NOVO CAGED/{}/2025{}/'.format(ano_atual,competencia))
except Exception as e:
   print("Ao tentar entrar na FTP, ocorreu um erro do tipo: {}".format(e))

# baixando os arquivos:
print('2/6 - Baixando os arquivos')
print('   2.1 - Baixando arquivo CAGEDMOV')
try:
   arquivo = ftp.nlst()
   arquivo_caged = [i for i in arquivo if 'CAGEDMOV' in i.upper() and i.endswith('.7z')]
   caged_mov = arquivo_caged[0]
   with open(caged_mov,'wb') as f:
        ftp.retrbinary('RETR {}'.format(caged_mov),f.write)
except Exception as e:
   print('Ao tentar baixar o CAGEDMOV, ocorreu um erro do tipo: {}'.format(e))

print('   2.2 - Baixando arquivo CAGEDFOR')
try:
   arquivo_caged = [i for i in arquivo if 'CAGEDFOR' in i.upper() and i.endswith('.7z')]
   caged_for = arquivo_caged[0]
   with open(caged_for,'wb') as f:
        ftp.retrbinary('RETR {}'.format(caged_for),f.write)
except Exception as e:
   print('Ao tentar baixar o CAGEDFOR, ocorreu um erro do tipo: {}'.format(e))

print('   2.3 - Baixando arquivo CAGEDEXC')
try:
   arquivo_caged = [i for i in arquivo if 'CAGEDEXC' in i.upper() and i.endswith('.7z')]
   caged_exc = arquivo_caged[0]
   with open(caged_exc,'wb') as f:
        ftp.retrbinary('RETR {}'.format(caged_exc),f.write)
   ftp.quit()
except Exception as e:
   print('Ao tentar baixar o CAGEDEXC, ocorreu um erro do tipo: {}'.format(e))

# Descompactando os arquivos:
print('3/6 - Descompactando os arquivos')
print('   3.1 - Descompactando o CAGEDMOV')
try:
   with py7zr.SevenZipFile(caged_mov, mode='r') as z:
      z.extractall()
except Exception as e:
   print('Ao tentar descompactar o arquivo MOV, ocorreu um erro do tipo: {}'.format(e))

print('   3.2 - Descompactando o CAGEDFOR')
try:
   with py7zr.SevenZipFile(caged_for, mode='r') as z:
      z.extractall()
except Exception as e:
   print('Ao tentar descompactar o arquivo FOR, ocorreu um erro do tipo: {}'.format(e))
   
print('   3.3 - Descompactando o CAGEDEXC')
try:
   with py7zr.SevenZipFile(caged_exc, mode='r') as z:
      z.extractall()
except Exception as e:
   print('Ao tentar descompactar o arquivo EXC, ocorreu um erro do tipo: {}'.format(e))

# Upload e tratamento dos microdados:
print('4/6 - Tratando os microdados')

## Definindo os dicionários
colunas = ['data_competencia','ano','mes','região','uf','município','seção','saldomovimentação','graudeinstrução','idade','raçacor','sexo','cbo2002ocupação']

dic_regiao = {"1": "Norte","2": "Nordeste","3": "Sudeste","4": "Sul","5": "Centro-Oeste"}

dic_secao = {
    "A": "Agricultura, pecuária e serviços relacionados","B": "Industria extrativa","C": "Industria de transformação","D": "Eletricidade e gás",
    "E": "Agua, esgoto, atividades de gestão de resíduos e descontaminação","F": "Construção",
    "G": "Comércio, reparação de veículos e motocicletas",
    "H": "Transporte, armazenagem e correio",
    "I": "Alojamento e alimentação",
    "J": "Informação e comunicação",
    "K": "Atividades financeiras, de seguros e serviços relacionados",
    "L": "Atividades imobiliárias",
    "M": "Atividades profissionais, científicas e técnicas",
    "N": "Atividades administrativas e serviços complementares",
    "O": "Administração pública, defesa e seguridade social",
    "P": "Educação",
    "Q": "Saúde humana e serviços sociais",
    "R": "Artes, cultura, esporte e recreação",
    "S": "Outras atividades de serviços",
    "T": "Serviços domésticos",
    "U": "Organismos internacionais e outras instituições extraterritoriais"}

dic_uf = {
    "11": "Rondônia", "12": "Acre", "13": "Amazonas", "14": "Roraima","15": "Pará", "16": "Amapá", "17": "Tocantins","21": "Maranhão", "22": "Piauí", "23": "Ceará", "24": "Rio Grande do Norte","25": "Paraíba", "26": "Pernambuco", "27": "Alagoas", "28": "Sergipe",
    "29": "Bahia","31": "Minas Gerais", "32": "Espírito Santo", "33": "Rio de Janeiro", "35": "São Paulo","41": "Paraná", "42": "Santa Catarina", "43": "Rio Grande do Sul","50": "Mato Grosso do Sul", "51": "Mato Grosso", "52": "Goiás", "53": "Distrito Federal"}

dic_graudeinstrucao = {
   "1": "Analfabeto","2": "Até 5º Incompleto","3": "5º Completo Fundamental","4": "6º a 9º Fundamental","5": "Fundamental Completo","6": "Médio Incompleto","7": "Médio Completo","8": "Superior Incompleto","9": "Superior Completo","10": "Mestrado","11": "Doutorado","80": "Pós-graduação completa"}

dic_raca = {"1": "Branca","2": "Preta","3": "Parda","4": "Amarela","5": "Indígena"}

dic_sexo = {"1": "Homem","3": "Mulher"}

rename_coluna = {
    'município': 'municipio','seção': 'secao','região': 'regiao','saldomovimentação': 'saldo_movimentacao','graudeinstrução': 'instrucao','raçacor': 'raca_cor','sexo': 'sexo','idade': 'idade','cbo2002ocupação': 'cbo'}

colunas_numericas = ['saldo_movimentacao', 'idade', 'ano', 'mes']

print('   4.1 - Tratando o arquivo CAGEDMOV')
try:
   caged_mov_txt = caged_mov.replace('.7z','.txt')
   BD_caged = pd.read_csv(caged_mov_txt, sep = ';', encoding = 'utf-8-sig', dtype=str)
   
   if 'competênciamov' in BD_caged.columns:
      BD_caged['ano'] = BD_caged['competênciamov'].str[:4]
      BD_caged['mes'] = BD_caged['competênciamov'].str[4:]

   BD_caged['data_competencia'] = pd.to_datetime(
          BD_caged['ano'] + '-' + BD_caged['mes'] + '-01'
      )
   
   colunas = [i for i in colunas if i in BD_caged.columns]
   BD_caged = BD_caged[colunas]
   
   BD_caged['região'] = BD_caged['região'].map(dic_regiao).fillna('Desconhecido')
   BD_caged = BD_caged[BD_caged['região'] == 'Nordeste'].copy()
  
   BD_caged['saldomovimentação'] = pd.to_numeric(BD_caged['saldomovimentação'], errors='coerce')
   BD_caged['idade'] = pd.to_numeric(BD_caged['idade'], errors='coerce')

   BD_caged['setor'] = np.select(
      [BD_caged['seção'].isin(['A']), BD_caged['seção'].isin(['B','C','D','E']), BD_caged['seção'].isin(['F']), BD_caged['seção'].isin(['G']),
       BD_caged['seção'].isin(['H','I','J','K','L','M','N','O','P','Q','R','S','T','U'])],
       ['Agricultura','Industria','Construção','Comércio','Serviços'],
       default='Desconhecidos')
   
   BD_caged['faixa_etaria'] = np.select(
      [BD_caged['idade'].isin([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]),
       BD_caged['idade'].isin([18,19,20,21,22,23,24]),
       BD_caged['idade'].isin([25,26,27,28,29]),
       BD_caged['idade'].isin([30,31,32,33,34,35,36,37,38,39]),
       BD_caged['idade'].isin([40,41,42,43,44,45,46,47,48,49]),
       BD_caged['idade'].isin([50,51,52,53,54,55,56,57,58,59,60,61,62,63,64]),
       BD_caged['idade'] >= 65],
       ['Até 17 anos','18 a 24 anos','25 a 29 anos','30 a 39 anos','40 a 49 anos','50 a 64 anos','65 anos ou mais'],
       default='Desconhecidos'
   )

   BD_caged['seção'] = BD_caged['seção'].map(dic_secao).fillna('Desconhecido')

   BD_caged['uf'] = BD_caged['uf'].map(dic_uf).fillna('Desconhecido')

   BD_caged['graudeinstrução'] = BD_caged['graudeinstrução'].map(dic_graudeinstrucao).fillna('Desconhecido')

   BD_caged['raçacor'] = BD_caged['raçacor'].map(dic_raca).fillna('Desconhecido')

   BD_caged['sexo'] = BD_caged['sexo'].map(dic_sexo).fillna('Desconhecido')

   BD_caged.rename(columns=rename_coluna, inplace=True)

   for i in colunas_numericas:
      BD_caged[i] = pd.to_numeric(BD_caged[i], errors='coerce').fillna(0).astype(int)

except Exception as e:
   print('Ao tentar tratar o CAGED MOV, ocorreu um erro do tipo: {}'.format(e))

print('   4.2 - Tratando o arquivo CAGEDFOR')
try:
   caged_for_txt = caged_for.replace('.7z','.txt')
   BD_caged_for = pd.read_csv(caged_for_txt, sep = ';', encoding = 'utf-8-sig', dtype=str)
   
   if 'competênciamov' in BD_caged_for.columns:
      BD_caged_for['ano'] = BD_caged_for['competênciamov'].str[:4]
      BD_caged_for['mes'] = BD_caged_for['competênciamov'].str[4:]

   BD_caged_for['data_competencia'] = pd.to_datetime(
          BD_caged_for['ano'] + '-' + BD_caged_for['mes'] + '-01'
      )

   colunas = [i for i in colunas if i in BD_caged_for.columns]
   BD_caged_for = BD_caged_for[colunas]
   
   BD_caged_for = BD_caged_for[BD_caged_for['ano'].isin(ano_filtro)].copy()

   BD_caged_for['região'] = BD_caged_for['região'].map(dic_regiao).fillna('Desconhecido')
   BD_caged_for = BD_caged_for[BD_caged_for['região'] == 'Nordeste'].copy()

   BD_caged_for['saldomovimentação'] = pd.to_numeric(BD_caged_for['saldomovimentação'], errors='coerce')
   BD_caged_for['idade'] = pd.to_numeric(BD_caged_for['idade'], errors='coerce')
   
   BD_caged_for['setor'] = np.select(
      [BD_caged_for['seção'].isin(['A']), BD_caged_for['seção'].isin(['B','C','D','E']), BD_caged_for['seção'].isin(['F']), BD_caged_for['seção'].isin(['G']),
       BD_caged_for['seção'].isin(['H','I','J','K','L','M','N','O','P','Q','R','S','T','U'])],
       ['Agricultura','Industria','Construção','Comércio','Serviços'],
       default='Desconhecidos')
   
   BD_caged_for['faixa_etaria'] = np.select(
      [BD_caged_for['idade'].isin([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]),
       BD_caged_for['idade'].isin([18,19,20,21,22,23,24]),
       BD_caged_for['idade'].isin([25,26,27,28,29]),
       BD_caged_for['idade'].isin([30,31,32,33,34,35,36,37,38,39]),
       BD_caged_for['idade'].isin([40,41,42,43,44,45,46,47,48,49]),
       BD_caged_for['idade'].isin([50,51,52,53,54,55,56,57,58,59,60,61,62,63,64]),
       BD_caged_for['idade'] >= 65],
       ['Até 17 anos','18 a 24 anos','25 a 29 anos','30 a 39 anos','40 a 49 anos','50 a 64 anos','65 anos ou mais'],
       default='Desconhecidos'
   )

   BD_caged_for['seção'] = BD_caged_for['seção'].map(dic_secao).fillna('Desconhecido')

   BD_caged_for['uf'] = BD_caged_for['uf'].map(dic_uf).fillna('Desconhecido')

   BD_caged_for['graudeinstrução'] = BD_caged_for['graudeinstrução'].map(dic_graudeinstrucao).fillna('Desconhecido')

   BD_caged_for['raçacor'] = BD_caged_for['raçacor'].map(dic_raca).fillna('Desconhecido')

   BD_caged_for['sexo'] = BD_caged_for['sexo'].map(dic_sexo).fillna('Desconhecido')

   BD_caged_for.rename(columns=rename_coluna, inplace=True)

   for i in colunas_numericas:
      BD_caged_for[i] = pd.to_numeric(BD_caged_for[i], errors='coerce').fillna(0).astype(int)

except Exception as e:
   print('Ao tentar tratar o CAGED FOR, ocorreu um erro do tipo: {}'.format(e))


print('   4.3 - Tratando o arquivo CAGEDEXC')
try:
   caged_exc_txt = caged_exc.replace('.7z','.txt')
   BD_caged_exc = pd.read_csv(caged_exc_txt, sep = ';', encoding = 'utf-8-sig', dtype=str)
   
   if 'competênciamov' in BD_caged_exc.columns:
      BD_caged_exc['ano'] = BD_caged_exc['competênciamov'].str[:4]
      BD_caged_exc['mes'] = BD_caged_exc['competênciamov'].str[4:]

   BD_caged_exc['data_competencia'] = pd.to_datetime(
          BD_caged_exc['ano'] + '-' + BD_caged_exc['mes'] + '-01'
      )

   colunas = [i for i in colunas if i in BD_caged_exc.columns]
   BD_caged_exc = BD_caged_exc[colunas]

   BD_caged_exc = BD_caged_exc[BD_caged_exc['ano'].isin(ano_filtro)].copy()

   BD_caged_exc['região'] = BD_caged_exc['região'].map(dic_regiao).fillna('Desconhecido')
   BD_caged_exc = BD_caged_exc[BD_caged_exc['região'] == 'Nordeste'].copy()
   
   BD_caged_exc['saldomovimentação'] = pd.to_numeric(BD_caged_exc['saldomovimentação'], errors='coerce')
   BD_caged_exc['idade'] = pd.to_numeric(BD_caged_exc['idade'], errors='coerce')

   BD_caged_exc['setor'] = np.select(
      [BD_caged_exc['seção'].isin(['A']), BD_caged_exc['seção'].isin(['B','C','D','E']), BD_caged_exc['seção'].isin(['F']), BD_caged_exc['seção'].isin(['G']),
       BD_caged_exc['seção'].isin(['H','I','J','K','L','M','N','O','P','Q','R','S','T','U'])],
       ['Agricultura','Industria','Construção','Comércio','Serviços'],
       default='Desconhecidos')
   
   BD_caged_exc['faixa_etaria'] = np.select(
      [BD_caged_exc['idade'].isin([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]),
       BD_caged_exc['idade'].isin([18,19,20,21,22,23,24]),
       BD_caged_exc['idade'].isin([25,26,27,28,29]),
       BD_caged_exc['idade'].isin([30,31,32,33,34,35,36,37,38,39]),
       BD_caged_exc['idade'].isin([40,41,42,43,44,45,46,47,48,49]),
       BD_caged_exc['idade'].isin([50,51,52,53,54,55,56,57,58,59,60,61,62,63,64]),
       BD_caged_exc['idade'] >= 65],
       ['Até 17 anos','18 a 24 anos','25 a 29 anos','30 a 39 anos','40 a 49 anos','50 a 64 anos','65 anos ou mais'],
       default='Desconhecidos'
   )

   BD_caged_exc['seção'] = BD_caged_exc['seção'].map(dic_secao).fillna('Desconhecido')

   BD_caged_exc['uf'] = BD_caged_exc['uf'].map(dic_uf).fillna('Desconhecido')

   BD_caged_exc['graudeinstrução'] = BD_caged_exc['graudeinstrução'].map(dic_graudeinstrucao).fillna('Desconhecido')

   BD_caged_exc['raçacor'] = BD_caged_exc['raçacor'].map(dic_raca).fillna('Desconhecido')

   BD_caged_exc['sexo'] = BD_caged_exc['sexo'].map(dic_sexo).fillna('Desconhecido')

   BD_caged_exc.rename(columns=rename_coluna, inplace=True)

   for i in colunas_numericas:
      BD_caged_exc[i] = pd.to_numeric(BD_caged_exc[i], errors='coerce').fillna(0).astype(int)

except Exception as e:
   print('Ao tentar tratar o CAGED EXC, ocorreu um erro do tipo: {}'.format(e))

# Unindo os banco de dados FOR e MOV
print('5/6 - Unindo os arquivos CAGEDFOR e CAGEDMOV')
try:
   BD_caged_join = pd.concat([BD_caged_for,BD_caged], ignore_index=True)
except Exception as e:
   print('Ao unir os bancos de dados, ocorreu um erro do tipo: {}'.format(e))

#Conectando com o PostgreSQL e enviando a Base de dados
print('6/6 - Enviando os arquivos para o PostgreSQL')

print('   6.1 - Conectando ao PostgreSQL')
try:
   engine =  create_engine(engine_sql)
except Exception as e:
   print('Ao tentar se conetar ao PostgreSQL, ocorreu um erro do tipo: {}'.format(e))

print('   6.1 - Processando arquivo CAGEDEXC')
try:
   BD_caged_exc.to_sql('tabela_temp_exc', engine, if_exists='replace',index=False)
   apagar = text(f"""
        WITH pares_para_apagar AS (
            SELECT
                m.ctid 
            FROM (
                SELECT 
                    ctid, 
                    ano, mes, municipio, saldo_movimentacao, idade, sexo, cbo,
                    ROW_NUMBER() OVER (
                        PARTITION BY ano, mes, municipio, saldo_movimentacao, idade, sexo, cbo
                        ORDER BY ctid
                    ) as rn_main
                FROM ft_caged
            ) AS m
            JOIN (
                SELECT 
                    ano, mes, municipio, saldo_movimentacao, idade, sexo, cbo,
                    ROW_NUMBER() OVER (
                        PARTITION BY ano, mes, municipio, saldo_movimentacao, idade, sexo, cbo 
                        ORDER BY 1
                    ) as rn_dex
                FROM tabela_temp_exc
            ) AS d
            ON  m.ano = d.ano
            AND m.mes = d.mes
            AND m.municipio = d.municipio
            AND m.saldo_movimentacao = d.saldo_movimentacao
            AND m.idade = d.idade
            AND m.sexo = d.sexo
            AND m.cbo = d.cbo
            AND m.rn_main = d.rn_dex
        )
        DELETE FROM ft_caged
        WHERE ctid IN (SELECT ctid FROM pares_para_apagar);
    """)
   
   with engine.begin() as conectar:
      resultado = conectar.execute(apagar)
      conectar.execute(text("DROP TABLE tabela_temp_exc"))

except Exception as e:
   print("Ao tentar apagar os erros da compeência anterios, ocorreu um erro do tipo: {}".format(e))

print('   6.2 - Processando arquivos CAEDMOV E CAGEDFOR')
try:
   BD_caged_join.to_sql('ft_caged', engine, if_exists='append', index=False)
except Exception as e:
   print("ao tentar enviar o bando de dados MOV e FOR, ocorreu um erro do tipo {}".format(e))
print('Atualização realizada com sucesso, código finalizado.')