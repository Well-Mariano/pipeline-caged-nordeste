WITH pares_para_apagar AS (
            SELECT
                m.ctid 
            FROM (
                SELECT 
                    ctid, 
                    ano, mes, "município", "saldomovimentação", idade, sexo, cbo,
                    ROW_NUMBER() OVER (
                        PARTITION BY ano, mes, "município", "saldomovimentação", idade, sexo, cbo 
                        ORDER BY ctid
                    ) as rn_main
                FROM caged_movimentacao
            ) m
            JOIN (
                SELECT 
                    ano, mes, "município", "saldomovimentação", idade, sexo, cbo,
                    ROW_NUMBER() OVER (
                        PARTITION BY ano, mes, "município", "saldomovimentação", idade, sexo, cbo 
                        ORDER BY 1
                    ) as rn_dex
                FROM tabela_temp_exc
            ) d
            ON  m.ano = d.ano
            AND m.mes = d.mes
            AND m."município" = d."município"
            AND m."saldomovimentação" = d."saldomovimentação"
            AND m.idade = d.idade
            AND m.sexo = d.sexo
            AND m.cbo = d.cbo
            AND m.rn_main = d.rn_dex
        )
        DELETE FROM caged_movimentacao
        WHERE ctid IN (SELECT ctid FROM pares_para_apagar);