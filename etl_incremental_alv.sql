/*
	ETL incremental do DW da ALV.
*/

-- Reinicia esquema de auditoria.
Drop schema if exists audit cascade;
create schema audit;

set search_path=audit;

/*
	Gravar alterações no DW em uma tabela.
*/

create table audit.historico_mudancas_alv(
	schema_name text not null,
	table_name text not null,
	user_name text,
	action_tstamp timestamp with time zone not null default current_timestamp,
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
		
		RETURN NEW;
	ELSIF (TG_OP = 'DELETE') then
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
	ELSIF (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);

		insert into audit.historico_mudancas_alv(
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

	-- Tratando exceções de erros nos dados.
	EXCEPTION
		-- Erro de tipo.
		WHEN data_exception THEN
			RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
		-- Violação de unicidade.
		WHEN unique_violation THEN
			RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
		WHEN others THEN
			RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
		RETURN NULL;
		-- SQLSTATE e SQLERRM são variáveis que tem código de estado do erro sql e a mensgagem de erro, respectivamente.
END;

$body$ LANGUAGE plpgsql

SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- =======================================================================================================================

/*
	Triggers para as mudanças nas tabelas específicas.
	Cada trigger é disparado quando a tabela original, no esquema relacional
	é alterada com novos dados e é responsável por registrar somente esses 
	novos dados na tabela de auditoria para que seja possível a atualização
	do DW.
	Aqui estão sendo criadas triggers apenas para as mudanças de tabelas
	que importam para o DW.
*/

-- Esses triggers salvam as mudanças na tabela de auditoria de mudanças no geral.
-- Produtora
CREATE TRIGGER Produtora_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Produtora
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- Filme
CREATE TRIGGER Filme_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Filme
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- Usuario
CREATE TRIGGER Usuario_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Usuario
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- Filme_GeneroFilme
CREATE TRIGGER Filme_GeneroFilme_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Filme_GeneroFilme
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- Assinatura
CREATE TRIGGER Assinatura_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Assinatura
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- UsrPagto
CREATE TRIGGER UsrPagto_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.UsrPagto
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();

-- Avaliacao
CREATE TRIGGER Avaliacao_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.Avaliacao
-- Executa a função que registra a operação.
FOR EACH ROW EXECUTE FUNCTION audit.if_modified_func();


/*
	Tabelas de auditoria temporárias que ajudarão na inserção dos novos dados no dw.
*/

-- Produtora
create table audit.ins_Produtora as select * from alv.Produtora where 1=0;

create or replace function audit.ins_Produtora_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_Produtora values
		(
			NEW.ProdutoraID,
			NEW.ProdutoraNome
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Produtora_insert_trg
AFTER INSERT ON alv.Produtora
FOR EACH ROW EXECUTE FUNCTION audit.ins_Produtora_func();

-- Filme
create table audit.ins_Filme as select * from alv.Filme where 1=0;

create or replace function audit.ins_Filme_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
	v_new_data := ROW(NEW.*);
	insert into audit.ins_Filme values
	(
		NEW.FilmeID,
		NEW.DuracaoMin,
		NEW.FilmeNome,
		NEW.AnoDeLancamento,
		NEW.ProdutoraID
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Filme_insert_trg
AFTER INSERT ON alv.Filme
FOR EACH ROW EXECUTE FUNCTION audit.ins_Filme_func();

-- Usuario
create table audit.ins_Usuario as select * from alv.Usuario where 1=0;

create or replace function audit.ins_Usuario_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_Usuario values
		(
			NEW.UsuarioID,
			NEW.Email,
			NEW.Telefone,
			NEW.DataVencimento,
			NEW.CodigoDeSeguranca,
			NEW.NumeroDoCartao,
			NEW.NomeDoProprietario,
			NEW.Senha,
			NEW.UsuarioNome,
			NEW.Bairro,
			NEW.Municipio,
			NEW.Estado,
			NEW.Logradouro
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Usuario_insert_trg
AFTER INSERT ON alv.Usuario
FOR EACH ROW EXECUTE FUNCTION audit.ins_Usuario_func();

-- Filme_GeneroFilme
create table audit.ins_Filme_GeneroFilme as select * from alv.Filme_GeneroFilme where 1=0;

create or replace function audit.ins_Filme_GeneroFilme_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_Filme_GeneroFilme values
		(
			NEW.FilmeID,
			NEW.GeneroFilme
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Filme_GeneroFilme_insert_trg
AFTER INSERT ON alv.Filme_GeneroFilme
FOR EACH ROW EXECUTE FUNCTION audit.ins_Filme_GeneroFilme_func();

-- Assinatura
create table audit.ins_Assinatura as select * from alv.Assinatura where 1=0;

create or replace function audit.ins_Assinatura_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_Assinatura values
		(
			NEW.AssinaturaID,
			NEW.DataInicio,
			NEW.DataFim,
			NEW.Status,
			NEW.PlanoID
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Assinatura_insert_trg
AFTER INSERT ON alv.Assinatura
FOR EACH ROW EXECUTE FUNCTION audit.ins_Assinatura_func();

-- UsrPagto
create table audit.ins_UsrPagto as select * from alv.UsrPagto where 1=0;

create or replace function audit.ins_UsrPagto_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_UsrPagto values
		(
			NEW.UsuarioID,
			NEW.AssinaturaID,
			NEW.ValorPago,
			NEW.DataPagto
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER UsrPagto_insert_trg
AFTER INSERT ON alv.UsrPagto
FOR EACH ROW EXECUTE FUNCTION audit.ins_UsrPagto_func();

-- Avaliacao
create table audit.ins_Avaliacao as select * from alv.Avaliacao where 1=0;

create or replace function audit.ins_Avaliacao_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_Avaliacao values
		(
			NEW.AvaliacaoID,
			NEW.Comentario,
			NEW.AvaliacaoData,
			NEW.Nota,
			NEW.UsuarioID,
			NEW.FilmeID,
			NEW.AssinaturaID
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
$body$ LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, audit;

-- salvando exatamente a linha recém inserida como uma nova linha em uma tabela "espelho"
CREATE TRIGGER Avaliacao_insert_trg
AFTER INSERT ON alv.Avaliacao
FOR EACH ROW EXECUTE FUNCTION audit.ins_Avaliacao_func();


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
    fg.GeneroFilme
FROM
    alv.Filme_GeneroFilme fg inner join audit.ins_Filme f on f.FilmeID = fg.FilmeID;

-- Atualizando dimensão de usuario.
INSERT INTO dw_alv.Usuario
SELECT
    gen_random_uuid(),
    u.UsuarioID,
    u.Email,
    u.Telefone,
    u.DataVencimento,
    u.CodigoDeSeguranca,
    u.NumeroDoCartao,
    u.NomeDoProprietario,
    u.Senha,
    u.UsuarioNome
FROM
    audit.ins_Usuario u;

-- Atualizando dimensão de endereco.
INSERT INTO dw_alv.Endereco
SELECT
    gen_random_uuid(),
    u.Estado,
    u.Municipio,
    u.Bairro,
    u.Logradouro
FROM
    alv.Usuario u inner join audit.ins_Usuario au ON u.UsuarioID = au.UsuarioID;

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
    audit.ins_Avaliacao a INNER JOIN alv.Filme f ON a.FilmeID = f.FilmeID
    INNER JOIN alv.Filme_GeneroFilme g ON g.FilmeID = f.FilmeID
    INNER JOIN alv.Produtora p ON p.ProdutoraID = f.ProdutoraID
    INNER JOIN alv.Usuario u ON a.UsuarioID = u.UsuarioID
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
    audit.ins_UsrPagto up INNER JOIN alv.Usuario u ON up.UsuarioID = u.UsuarioID
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

-- No fim da adição dos novos dados as tabelas temporárias de auditoria devem
-- ser truncadas.
truncate table audit.ins_Produtora;
truncate table audit.ins_Filme;
truncate table audit.ins_Usuario;
truncate table audit.ins_Filme_GeneroFilme;
truncate table audit.ins_Assinatura;
truncate table audit.ins_UsrPagto;
truncate table audit.ins_Avaliacao;