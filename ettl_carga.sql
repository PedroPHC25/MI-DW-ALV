set search_path to dw_alv;

truncate table Receita;
truncate table Avaliacao;

delete from Calendario;
delete from Endereco;
delete from Filme;
delete from Genero;
delete from Produtora;
delete from Usuario;

set search_path to alv;

INSERT INTO dw_alv.Produtora
SELECT 
    gen_random_uuid(),
    p.ProdutoraID,
    p.ProdutoraNome
FROM 
    produtora p;

INSERT INTO dw_alv.Filme
SELECT
    gen_random_uuid(),
    f.FilmeID,
    f.DuracaoMin,
    f.FilmeNome,
    f.AnoDeLancamento
FROM
    filme f;

INSERT INTO dw_alv.Genero
SELECT DISTINCT ON (GeneroFilme)
    gen_random_uuid(),
    fg.GeneroFilme
FROM
    Filme_GeneroFilme fg;

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
    Usuario u;

INSERT INTO dw_alv.Endereco
SELECT
    gen_random_uuid(),
    u.Estado,
    u.Municipio,
    u.Bairro,
    u.Logradouro
FROM
    Usuario u;

-- INSERT INTO dw_alv.Calendario
-- SELECT
--     gen_random_uuid(),
--     d.DataCompleta

-- FROM(
--     SELECT AvaliacaoData AS DataCompleta FROM Avaliacao
--     UNION ALL
--     SELECT DataPagto AS DataCompleta FROM  UsrPagto
-- ) AS d