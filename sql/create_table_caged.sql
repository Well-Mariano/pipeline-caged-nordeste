create table ft_caged (
	id_registro SERIAL PRIMARY KEY,
	regiao integer,
	uf integer,
	municipio integer,
	secao char(2),
	saldo_movimentacao integer,
	cbo varchar(6),
	instrucao integer,
	idade integer,
	raca_cor integer,
	sexo integer,
	ano integer,
	mes integer,
	data_competencia date,
	faixa_etaria varchar(15)
);