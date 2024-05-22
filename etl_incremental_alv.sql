/*
	ETL incremental do DW da ALV.
*/

Drop schema if exists audit;
create schema audit;
set search_path=audit;

/*
Gravar alterações no DW em uma tabela.
*/

create table audit.historico_mudancas_alv(
	schema_name text not null,
	table_name text not null,
	user_name text,
	action_tstamp timestam with time zone not null default current_timestamp,
	action TEXT NOT NULL check (action in ('I', 'D', 'U')),
	original_data text,
	new_data text,
	query text
) with (fillfactor=100);

/*
	Função de trigger que grava as alterações no geral.
*/

CREATE OR REPLACE FUNCTION audit.if_modified_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN

IF (TG_OP = 'UPDATE') then
	v_old_data := ROW(OLD.*);
	v_new_data := ROW(NEW.*);

	insert into audit.historico_mudancas_alv(
		schema_name,
		table_name,
		user_name,
		action,
		original_data,
		new_data,
		query
	)
	VALUES(
		TG_TABLE_SCHEMA::TEXT,
		TG_TABLE_NAME::TEXT,
		session_user::TEXT,
		substring(TG_OP,1,1),
		v_old_data,
		v_new_data,
		current_query()
	);
	
	RETURN NEW
elsif (TG_OP = 'DELETE') then
	v_old_data := ROW(OLD.*);

	insert into audit.historico_mudancas_alv(
		schema_name,
		table_name,
		user_name,
		action,
		original_data,
		query
	)
	values(
		TG_TABLE_SCHEMA::TEXT,
		TG_TABLE_NAME::TEXT,
		session_user::TEXT,
		substring(TG_OP,1,1),
		v_old_data,
		current_query()
	);
	RETURN OLD;
elsif (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);

	insert into audit.historico_mudancas_zagi(
		schema_name,
		table_name,
		user_name,
		action,
		new_data,
		query
	)
	values(
		TG_TABLE_SCHEMA::TEXT,
		TG_TABLE_NAME::TEXT,
		session_user::TEXT,
		substring(TG_OP,1,1),
		v_new_data,
		current_query()
	);
	RETURN NEW;
else
	RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
	RETURN NULL;
end if;

EXCEPTION
	WHEN data_exception THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
	RETURN NULL;
	WHEN unique_violation THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
	RETURN NULL;
	WHEN others THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
	RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

/*
	Triggers para as mudanças nas tabelas específicas.
*/

-- Modificações em Avaliacoes
CREATE TRIGGER Avaliacoes_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Avaliacoes
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Modificações em Receita
CREATE TRIGGER Receita_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Receita
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();


/*
	Trigger para salvar inserções na tabela.
*/
-- Avaliacoes.
CREATE TABLE audit.ins_Avaliacoes AS SELECT *
from dw_alv.Avaliacoes where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Avaliacoes_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Avaliacoes values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
		RETURN NEW;
	else
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
		RETURN NULL;
	end if;

EXCEPTION
	WHEN data_exception THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	WHEN unique_violation THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	WHEN others THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	END;
	$body$
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Avaliacoes_insert_trg
AFTER INSERT ON dw_alv.Avaliacoes
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Avaliacoes_func();


-- Receita.
CREATE TABLE audit.ins_Receita AS SELECT *
from dw_alv.Receita where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Receita_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Receita                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
		RETURN NEW;
	else
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
		RETURN NULL;
	end if;

EXCEPTION
	WHEN data_exception THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	WHEN unique_violation THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	WHEN others THEN
		RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
	END;
	$body$
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Receita_insert_trg
AFTER INSERT ON dw_alv.Receita
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Receita_func();

-- Adicionar para dimensões?

-- Atualizando dimensão de calendário.
INSERT INTO dw_alv.Calendario
SELECT
    gen_random_uuid(),
    cal.DataCompleta,
    cal.DiaSemana,
    cal.Dia,
    cal.Mes,
    cal.Trimestre,
    cal.Ano
FROM(
    SELECT DISTINCT
    CAST(d.DataCompleta AS DATE) AS DataCompleta,
    to_char(d.DataCompleta, 'DY') AS DiaSemana,
    EXTRACT(day FROM d.DataCompleta) AS Dia,
    to_char(d.DataCompleta,  'MM') AS Mes,
    CAST(to_char(d.DataCompleta, 'Q')AS int) AS Trimestre,
    EXTRACT(year FROM d.DataCompleta) AS Ano
    FROM
    (   
        SELECT AvaliacaoData AS DataCompleta FROM alv.Avaliacao
        UNION ALL
        SELECT DataPagto AS DataCompleta FROM alv.UsrPagto
    ) AS d) AS cal
WHERE CAST(cal.DataCompleta AS DATE) NOT IN (SELECT DataCompleta FROM dw_alv.Calendario);

-- Inserindo na tabela de avaliações novos registros.
INSERT INTO dw_alv.Avaliacoes
SELECT
    a.AvaliacaoID,
    dwg.GeneroKey,
    dwf.FilmeKey,
    dwp.ProdutoraKey,
    dwc.CalendarioKey,
    dwu.UsuarioKey,
    a.Nota,
    CAST(to_char(a.AvaliacaoData, 'HH24:MI:SS') AS TIME) AS HORA
FROM
    Avaliacao a INNER JOIN Filme f ON a.FilmeID = f.FilmeID
    INNER JOIN Filme_GeneroFilme g ON g.FilmeID = f.FilmeID
    INNER JOIN Produtora p ON p.ProdutoraID = f.ProdutoraID
    INNER JOIN Usuario u ON a.UsuarioID = u.UsuarioID
    INNER JOIN dw_alv.Genero dwg ON g.GeneroFilme = dwg.GeneroNome
    INNER JOIN dw_alv.Filme dwf ON f.FilmeID = dwf.FilmeID
    INNER JOIN dw_alv.Produtora dwp ON p.ProdutoraID = dwp.ProdutoraID
    INNER JOIN dw_alv.Usuario dwu ON u.UsuarioID = dwu.UsuarioID
    INNER JOIN dw_alv.Calendario dwc ON a.AvaliacaoData = dwc.DataCompleta
EXCEPT
SELECT 
    AvaliacaoID,
    GeneroKey,
    FilmeKey,
    ProdutoraKey,
    CalendarioKey,
    UsuarioKey,
    Nota,
    Hora
FROM
    dw_alv.Avaliacoes;


-- Inserindo na tabela de avaliações novos registros.