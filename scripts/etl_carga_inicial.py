# Bibliotecas
import ftplib
import os
import py7zr
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Entrando na FTP do MTE:
try:
   ftp = ftplib.FTP('ftp.mtps.gov.br')
   ftp.encoding = 'latin-1'
   ftp.login()
   ftp.cwd('/pdet/microdados/NOVO CAGED/2025/202501/')
except Exception as e:
   print("Ao tentar entrar na FTP, ocorreu um erro do tipo: {}".format(e))

# baixando o arquivo:
try:
   arquivo = ftp.nlst()
   arquivo_caged = [i for i in arquivo if 'CAGEDMOV' in i.upper() and i.endswith('.7z')]
   caged = arquivo_caged[0]
   with open(caged,'wb') as f:
        ftp.retrbinary('RETR {}'.format(caged),f.write)
   ftp.quit()
except Exception as e:
   print('Ao tentar baixar o microdados, ocorreu um erro do tipo: {}'.format(e))

# Descompactando o arquivo:
try:
   with py7zr.SevenZipFile(caged, mode='r') as z:
      z.extractall()
except Exception as e:
   print('Ao tentar descompactar o arquivo, ocorreu um erro do tipo: {}'.format(e))
   
# Upload e tratamento dos microdados:
try:
   caged_txt = caged.replace('.7z','.txt')
   BD_caged = pd.read_csv(caged_txt, sep = ';', encoding = 'utf-8-sig', dtype=str)
   
   if 'competênciamov' in BD_caged.columns:
      BD_caged['ano'] = BD_caged['competênciamov'].str[:4]
      BD_caged['mes'] = BD_caged['competênciamov'].str[4:]
   
   BD_caged['data_competencia'] = pd.to_datetime(
          BD_caged['ano'] + '-' + BD_caged['mes'] + '-01'
      )
   
   if 'cbo2002ocupação' in BD_caged.columns:
        BD_caged.rename(columns={'cbo2002ocupação': 'cbo'}, inplace=True)
   
   colunas = ['data_competencia','ano','mes','região','uf','município','seção','saldomovimentação','graudeinstrução','idade','raçacor','sexo','cbo']
   colunas = [i for i in colunas if i in BD_caged.columns]
   BD_caged = BD_caged[colunas]

   dic_regiao = {"1": "Norte","2": "Nordeste","3": "Sudeste","4": "Sul","5": "Centro-Oeste"}
   BD_caged['região'] = BD_caged['região'].map(dic_regiao).fillna('Desconhecido')
   BD_caged = BD_caged[BD_caged['região'] == 'Nordeste'].copy()

   BD_caged['saldomovimentação'] = pd.to_numeric(BD_caged['saldomovimentação'], errors='coerce')
   BD_caged['idade'] = pd.to_numeric(BD_caged['idade'], errors='coerce')
   
   BD_caged['setor'] = np.select(
      [BD_caged['seção'].isin(['A']), BD_caged['seção'].isin(['B','C','D','E']), BD_caged['seção'].isin(['F']), BD_caged['seção'].isin(['G']),
       BD_caged['seção'].isin(['H','I','J','K','L','M','N','O','P','Q','R','S','T','U'])],
       ['Agricultura','Industria','Construção','Comércio','Serviços'],
       default='Desconhecidos'
   )

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

   dic_secao = {
    "A": "Agricultura, pecuária e serviços relacionados",
    "B": "Industria extrativa",
    "C": "Industria de transformação",
    "D": "Eletricidade e gás",
    "E": "Agua, esgoto, atividades de gestão de resíduos e descontaminação",
    "F": "Construção",
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
    "U": "Organismos internacionais e outras instituições extraterritoriais"
    }
   BD_caged['seção'] = BD_caged['seção'].map(dic_secao).fillna('Desconhecido')

   dic_uf = {
    "11": "Rondônia", "12": "Acre", "13": "Amazonas", "14": "Roraima","15": "Pará", "16": "Amapá", "17": "Tocantins","21": "Maranhão", "22": "Piauí", "23": "Ceará", "24": "Rio Grande do Norte","25": "Paraíba", "26": "Pernambuco", "27": "Alagoas", "28": "Sergipe",
    "29": "Bahia","31": "Minas Gerais", "32": "Espírito Santo", "33": "Rio de Janeiro", "35": "São Paulo","41": "Paraná", "42": "Santa Catarina", "43": "Rio Grande do Sul","50": "Mato Grosso do Sul", "51": "Mato Grosso", "52": "Goiás", "53": "Distrito Federal"}
   BD_caged['uf'] = BD_caged['uf'].map(dic_uf).fillna('Desconhecido')

   dic_graudeinstrucao = {"1": "Analfabeto","2": "Até 5º Incompleto","3": "5º Completo Fundamental","4": "6º a 9º Fundamental","5": "Fundamental Completo","6": "Médio Incompleto","7": "Médio Completo","8": "Superior Incompleto","9": "Superior Completo","10": "Mestrado","11": "Doutorado","80": "Pós-graduação completa"}
   BD_caged['graudeinstrução'] = BD_caged['graudeinstrução'].map(dic_graudeinstrucao).fillna('Desconhecido')

   dic_raca = {"1": "Branca","2": "Preta","3": "Parda","4": "Amarela","5": "Indígena"}
   BD_caged['raçacor'] = BD_caged['raçacor'].map(dic_raca).fillna('Desconhecido')

   dic_sexo = {"1": "Homem","3": "Mulher"}
   BD_caged['sexo'] = BD_caged['sexo'].map(dic_sexo).fillna('Desconhecido')

except Exception as e:
   print('Ao tentar tratar os microsdados, ocorreu um erro do tipo: {}'.format(e))

# Conectando com o PostgreSQL e enviando a Base de dados
try:
   engine = create_engine('postgresql://postgres:NovaSenha123@localhost/projeto_caged')

   insp = inspect(engine)


   BD_caged.to_sql('caged_movimentacao', engine, if_exists='append', index=False)
   print('Código finalizado')
except Exception as e:
   print("Ao realizar o envio para o SQL ocorreu o erro: {}".format(e))
