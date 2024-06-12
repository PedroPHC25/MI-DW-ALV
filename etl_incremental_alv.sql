/*
	ETL incremental do DW da ALV.
*/

-- Reinicia esquema de auditoria.
Drop schema if exists audit cascade;
create schema audit;

set search_path=audit;

/*
	Todas as linhas até a 542 são a crição de funções e triggers que salvam as alterações feitas no esquema relacional nas tabelas de auditoria. Dessas, a maioria é feita de tabelas temporárias que tem como objetivo armazenar as últimas mudanças até que o próximo ciclo de adição de dados no etl incremental seja rodado.
	A partir da linha 542 são rodadas as querys que adicionam os novos dados ao DW baseados nas tabelas de auditoria temporárias criadas. Após essa adição dos dados, as tabelas de auditoria temporárias são todas truncadas para que se inicie um novo ciclo.
*/

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

-- Tabela de dados externos
CREATE TRIGGER AvaliacoesIMDb_if_modified_trig
-- Depois da alguma operação de inserção, atualização ou deleção.
AFTER INSERT OR UPDATE OR DELETE ON alv.AvaliacoesIMDb
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

CREATE TRIGGER Avaliacao_insert_trg
AFTER INSERT ON alv.Avaliacao
FOR EACH ROW EXECUTE FUNCTION audit.ins_Avaliacao_func();

-- Tabela de dados externos
create table audit.ins_AvaliacoesIMDb as select * from alv.AvaliacoesIMDb where 1=0;

create or replace function audit.ins_AvaliacoesIMDb_func() returns trigger as $body$
DECLARE
	v_old_data TEXT;
	v_new_data TEXT;
BEGIN
	if (TG_OP = 'INSERT') then
		v_new_data := ROW(NEW.*);
		insert into audit.ins_AvaliacoesIMDb values
		(
			NEW.FilmeNome,
			NEW.Nota
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
CREATE TRIGGER AvaliacoesIMDb_insert_trg
AFTER INSERT ON alv.AvaliacoesIMDb
FOR EACH ROW EXECUTE FUNCTION audit.ins_AvaliacoesIMDb_func();

-- Inserção de novos valores no esquema relacional.
SET search_path TO alv;

INSERT INTO Usuario (UsuarioID, Email, Telefone, DataVencimento, CodigoDeSeguranca, NumeroDoCartao, NomeDoProprietario, Senha, UsuarioNome, Bairro, Municipio, Estado, Logradouro) VALUES
('U4', 'luiza.amaral@example.com', '27987654321', '2028-05-24', 112, '4555555555555555', 'Luiza Amaral', 'password112', 'Luiza Amaral', 'Jardim da Penha', 'Vitória', 'ES', 'Rua Marquês de Olinda 101'),
('U5', 'edna.santos@example.com', '41987654321', '2029-05-23', 101, '4444444444444444', 'Edna Santos', 'password101', 'Edna Santos', 'Boa Vista', 'Curitiba', 'PR', 'Rua Lodovico Geronazzo 101'),
('U6', 'sérgio.machado@example.com', '48987654321', '2030-10-10', 131, '4666666666666666', 'Sérgio Machado', 'password131', 'Sérgio Machado', 'Itacorubi', 'Florianópolis', 'SC', 'Avenida Buriti 131');

INSERT INTO Produtora (ProdutoraID, ProdutoraNome) VALUES
('P4', 'VideoFilmes'),
('P5', 'Zencrane Filmes'),
('P6', 'África Filmes');

INSERT INTO Filme (FilmeID, DuracaoMin, FilmeNome, AnoDeLancamento, ProdutoraID) VALUES
('F4', 113, 'Central do Brasil', 1998, 'P4'),
('F5', 113, 'Estômago', 2007, 'P5'),
('F6', 114, 'Que Horas Ela Volta?', 2015, 'P6');

INSERT INTO Filme_GeneroFilme (FilmeID, GeneroFilme) VALUES
('F4', 'Drama'),
('F4', 'Comédia'),
('F4', 'Aventura'),
('F5', 'Drama'),
('F5', 'Comédia'),
('F6', 'Comédia'),
('F6', 'Drama'),
('F6', 'Melodrama');

INSERT INTO Filme_DiretorFilme (FilmeID, DiretorFilme) VALUES
('F4', 'Walter Salles'),
('F5', 'Marcos Jorge'),
('F6', 'Anna Muylaert');

INSERT INTO Filme_AtorFilme (FilmeID, AtorFilme) VALUES
('F4', 'Fernanda Montenegro'),
('F4', 'Vinícius de Oliveira'),
('F5', 'João Miguel'),
('F5', 'Fabiula Nascimento'),
('F6', 'Regina Casé'),
('F6', 'Camila Márdila');

INSERT INTO Assinatura (AssinaturaID, DataInicio, DataFim, Status, PlanoID) VALUES
('Ass4', '2023-11-10', '2024-11-10', 'Ativo', 'Pl1'),
('Ass5', '2024-05-11', '2024-05-11', 'Ativo', 'Pl2'),
('Ass6', '2023-02-14', '2024-02-14', 'Desativado', 'Pl3');

INSERT INTO UsrPagto (UsuarioID, AssinaturaID, ValorPago, DataPagto) VALUES
('U4', 'Ass4', 10, '2023-11-24 21:12:23-03'),
('U5', 'Ass5', 30, '2024-05-25 00:34:31-03'),
('U6', 'Ass6', 40, '2023-02-28 08:42:59-03');

INSERT INTO Assiste (UsuarioID, FilmeID, Data) VALUES
('U4', 'F1', '2023-05-18'),
('U4', 'F4', '2024-01-29'),
('U5', 'F5', '2023-07-16'),
('U6', 'F6', '2023-09-02');

INSERT INTO Avaliacao (AvaliacaoID, Comentario, AvaliacaoData, Nota, UsuarioID, FilmeID, AssinaturaID) VALUES
('A4', 'Filme impecável com atuações maravilhosas. Me emocionei muito!', '2024-01-29 01:29:23-03', 10, 'U4', 'F4', 'Ass4'),
('A5', 'A intercalação entre passado, presente e futuro neste filme torna-o uma experiência única.', '2023-07-16 16:07:21-03', 9, 'U5', 'F5', 'Ass5'),
('A6', NULL, '2023-09-02 09:02:23-03', 8, 'U6', 'F6', 'Ass6');

INSERT INTO FilmPagtoRoy (FilmeID, ValorPagto, DataPagto) VALUES
('F4', 500, '2000-09-14'),
('F5', 700, '2009-01-23'),
('F6', 600, '2017-01-28');

-- Inserindo os dados de fonte externa.
INSERT INTO AvaliacoesIMDb (filmenome, nota) VALUES
('Harry Potter e a Pedra Filosofal', 7.3),
('Interestelar', 8.7),
('Central do Brasil', 8.0),
('Harry Potter e a Pedra Filosofal', 7.6),
('Estômago', 7.8),
('Jurassic Park - O Parque dos Dinossauros', 8.2),
('Que Horas Ela Volta?', 7.7);

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
-- Mudando estrutura da tabela de filmes no dw.
ALTER TABLE dw_alv.Filme
ADD COLUMN NotaIMDb REAL;

-- Atualizando linhas que já existem
UPDATE dw_alv.Filme
SET NotaIMDb = AvaliacoesIMDb.nota
FROM alv.AvaliacoesIMDb
WHERE dw_alv.Filme.FilmeNome = alv.AvaliacoesIMDb.FilmeNome

UPDATE dw_alv.Filme
SET NotaIMDb = 7.3
WHERE FilmeNome = 'Harry Potter e a Pedra Filosofal';
UPDATE dw_alv.Filme
SET NotaIMDb = 8.7
WHERE FilmeNome = 'Interestelar';
UPDATE dw_alv.Filme
SET NotaIMDb = 8.2
WHERE FilmeNome = 'Jurassic Park - O Parque dos Dinossauros';

-- Essa query que adiciona também os dados de fonte externa.
INSERT INTO dw_alv.Filme
SELECT
    gen_random_uuid(),
    f.FilmeID,
    f.DuracaoMin,
    f.FilmeNome,
    f.AnoDeLancamento,
	imdb.nota
FROM
    audit.ins_filme f
INNER JOIN
	AvaliacoesIMDb imdb ON f.FilmeNome = imdb.FilmeNome;

-- Atualizando dimensão de genero.
INSERT INTO dw_alv.Genero (generokey, generonome)
SELECT DISTINCT ON (generonome)
    gen_random_uuid() AS generokey,
    fg.GeneroFilme AS generonome
FROM
    audit.ins_Filme_GeneroFilme fg
WHERE
    fg.GeneroFilme NOT IN (
        SELECT generonome FROM dw_alv.Genero
    );

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
-- Essa query adiciona também os dados de fonte externa.
INSERT INTO dw_alv.Avaliacao
SELECT
    a.AvaliacaoID,
    dwg.GeneroKey,
    dwf.FilmeKey,
    dwp.ProdutoraKey,
    dwc.CalendarioKey,
    dwu.UsuarioKey,
    a.Nota,
    CAST(to_char(a.AvaliacaoData, 'HH24:MI:SSOF') AS TIME WITH TIME ZONE) AS HORA
	-- dwf.notaimdb
FROM
    audit.ins_Avaliacao a INNER JOIN audit.ins_Filme f ON a.FilmeID = f.FilmeID
    INNER JOIN audit.ins_Filme_GeneroFilme g ON g.FilmeID = f.FilmeID
    INNER JOIN alv.Produtora p ON p.ProdutoraID = f.ProdutoraID
    INNER JOIN alv.Usuario u ON a.UsuarioID = u.UsuarioID
    INNER JOIN dw_alv.Genero dwg ON g.GeneroFilme = dwg.GeneroNome
    INNER JOIN dw_alv.Filme dwf ON f.FilmeID = dwf.FilmeID
    INNER JOIN dw_alv.Produtora dwp ON p.ProdutoraID = dwp.ProdutoraID
    INNER JOIN dw_alv.Usuario dwu ON u.UsuarioID = dwu.UsuarioID
    INNER JOIN dw_alv.Calendario dwc ON a.AvaliacaoData::date = dwc.DataCompleta
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
    -- NotaIMDb
FROM
    dw_alv.Avaliacao;

-- Inserindo na tabela de receita novos registros.
INSERT INTO dw_alv.Receita
SELECT
    up.AssinaturaID,
    dwu.UsuarioKey,
    dwc.CalendarioKey,
    dwe.EnderecoKey,
    up.ValorPago AS Valor,
    CAST(to_char(up.DataPagto, 'HH24:MI:SSOF') AS TIME WITH TIME ZONE) AS Hora
FROM
    audit.ins_UsrPagto up INNER JOIN alv.Usuario u ON up.UsuarioID = u.UsuarioID
    INNER JOIN dw_alv.Usuario dwu ON u.UsuarioID = dwu.UsuarioID
    INNER JOIN dw_alv.Endereco dwe ON u.Logradouro = dwe.Logradouro
    INNER JOIN dw_alv.Calendario dwc ON up.DataPagto::date = dwc.DataCompleta
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