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
	Função de trigger que grava as alterações do forma geral.
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
