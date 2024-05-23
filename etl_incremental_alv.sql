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

-- =======================================================================================================================

/*
	Triggers para as mudanças nas tabelas específicas.
*/

-- Tabelas de fato.
-- Modificações em Avaliacoes.
CREATE TRIGGER Avaliacoes_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Avaliacoes
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Modificações em Receita.
CREATE TRIGGER Receita_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Receita
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Tabelas de dimensão.
-- Produtora
CREATE TRIGGER Produtora_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Produtora
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Filme
CREATE TRIGGER Filme_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Filme
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Genero
CREATE TRIGGER Genero_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Genero
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Usuario
CREATE TRIGGER Usuario_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Usuario
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Endereco
CREATE TRIGGER Endereco_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Endereco
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Calendario
CREATE TRIGGER Calendario_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON dw_alv.Calendario
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();


/*
	Tabelas de Auditoria.
*/

-- Tabelas de fato.
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Receita_insert_trg
AFTER INSERT ON dw_alv.Receita
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Receita_func();

-- Tabelas de dimensão.
-- Produtora
CREATE TABLE audit.ins_Produtora AS SELECT *
from dw_alv.Produtora where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Produtora_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Produtora                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Produtora_insert_trg
AFTER INSERT ON dw_alv.Produtora
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Produtora_func();

-- Filme
CREATE TABLE audit.ins_Filme AS SELECT *
from dw_alv.Filme where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Filme_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Filme                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Filme_insert_trg
AFTER INSERT ON dw_alv.Filme
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Filme_func();

-- Genero
CREATE TABLE audit.ins_Genero AS SELECT *
from dw_alv.Genero where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Genero_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Genero                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Genero_insert_trg
AFTER INSERT ON dw_alv.Genero
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Genero_func();

-- Usuario
CREATE TABLE audit.ins_Usuario AS SELECT *
from dw_alv.Usuario where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Usuario_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Usuario                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Usuario_insert_trg
AFTER INSERT ON dw_alv.Usuario
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Usuario_func();

-- Endereco
CREATE TABLE audit.ins_Endereco AS SELECT *
from dw_alv.Endereco where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Endereco_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Endereco                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Endereco_insert_trg
AFTER INSERT ON dw_alv.Endereco
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Endereco_func();

-- Calendario
CREATE TABLE audit.ins_Calendario AS SELECT *
from dw_alv.Calendario where 1=0;

CREATE OR REPLACE FUNCTION audit.ins_Calendario_func()
RETURNS trigger AS $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
		insert into audit.ins_Calendario                                                                                                                                                                                                                                                                                                                                                                                    values (NEW.Avaliacaoid,NEW.Filmeid,NEW.Calendarioid);
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

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho".
CREATE TRIGGER Calendario_insert_trg
AFTER INSERT ON dw_alv.Calendario
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Calendario_func();


/*
	Atualizando tabelas do DW.
*/

-- Tabelas de dimensão.
-- Atualizando dimensão de produtora.
INSERT INTO dw_alv.Produtora
SELECT 
    gen_random_uuid(),
    p.ProdutoraID,
    p.ProdutoraNome
FROM 
    audit.ins_produtora p;

-- Atualizando dimensão de filme.
INSERT INTO dw_alv.Filme
SELECT
    gen_random_uuid(),
    f.FilmeID,
    f.DuracaoMin,
    f.FilmeNome,
    f.AnoDeLancamento
FROM
    audit.ins_filme f;

-- Atualizando dimensão de genero.
INSERT INTO dw_alv.Genero
SELECT DISTINCT ON (GeneroFilme)
    gen_random_uuid(),
    g.GeneroFilme
FROM
    audit.ins_Genero g;

-- Atualizando dimensão de usuario.


-- Atualizando dimensão de endereco.


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

-- Tabelas de fato
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


-- Inserindo na tabela de receita novos registros.
INSERT INTO dw_alv.Receita
SELECT
    up.AssinaturaID,
    dwu.UsuarioKey,
    dwc.CalendarioKey,
    dwe.EnderecoKey,
    up.ValorPago AS Valor,
    CAST(to_char(up.DataPagto, 'HH24:MI:SS') AS TIME) AS Hora
FROM
    alv.UsrPagto up INNER JOIN alv.Usuario u ON up.UsuarioID = u.UsuarioID
    INNER JOIN dw_alv.Usuario dwu ON u.UsuarioID = dwu.UsuarioID
    INNER JOIN dw_alv.Endereco dwe ON u.Logradouro = dwe.Logradouro
    INNER JOIN dw_alv.Calendario dwc ON up.DataPagto = dwc.DataCompleta
EXCEPT
SELECT 
    AssinaturaID,
    UsuarioKey,
    CalendarioKey,
    EnderecoKey,
    Valor,
    Hora
FROM
    dw_alv.Receita;