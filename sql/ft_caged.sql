create table public.ft_caged (
	id_registro SERIAL PRIMARY KEY,
	data_competencia date,
	ano integer,
	mes integer,
	regiao varchar(8),
	uf varchar(20),
	municipio varchar(7),
	secao char(100),
	saldo_movimentacao integer,
	instrucao varchar(50),
	idade integer,
	raca_cor varchar(50),
	sexo varchar(20),
	cbo varchar(6),
	setor varchar(100),
	faixa_etaria varchar(15)
);