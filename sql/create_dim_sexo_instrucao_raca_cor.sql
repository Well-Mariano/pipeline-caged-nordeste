CREATE TABLE dim_cbo (
    id_cbo integer PRIMARY KEY,
    descricao_instrucao varchar(150)
);

CREATE TABLE dim_raca_cor (
    id_raca_cor integer PRIMARY KEY,
    descricao_raca_cor varchar(50)
);

CREATE TABLE dim_sexo (
    id_sexo integer PRIMARY KEY,
    descricao_sexo varchar(50)
);