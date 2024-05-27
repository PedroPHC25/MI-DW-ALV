-- Criando um esquema para armazenar o Data Warehouse
set search_path to dw_alv;

-- Truncando as tabelas de fatos e deletando as de dimensão
truncate table Receita;
truncate table Avaliacao;

delete from Calendario;
delete from Endereco;
delete from Filme;
delete from Genero;
delete from Produtora;
delete from Usuario;

-- Mudando o search_path para onde os dados estão armazenados
set search_path to alv;

-- Adicionando dados à dimensão "Produtora"
INSERT INTO dw_alv.Produtora
SELECT 
    gen_random_uuid(),
    p.ProdutoraID,
    p.ProdutoraNome
FROM 
    produtora p;

-- Adicionando dados à dimensão "Filme"
INSERT INTO dw_alv.Filme
SELECT
    gen_random_uuid(),
    f.FilmeID,
    f.DuracaoMin,
    f.FilmeNome,
    f.AnoDeLancamento
FROM
    filme f;

-- Adicionando dados à dimensão "Genero"
INSERT INTO dw_alv.Genero
SELECT DISTINCT ON (GeneroFilme)
    gen_random_uuid(),
    fg.GeneroFilme
FROM
    Filme_GeneroFilme fg;

-- Adicionando dados à dimensão "Usuário"
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

-- Adicionando dados à dimensão "Endereço"
INSERT INTO dw_alv.Endereco
SELECT
    gen_random_uuid(),
    u.Estado,
    u.Municipio,
    u.Bairro,
    u.Logradouro
FROM
    Usuario u;

-- Adicionando dados à dimensão "Calendário"
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
        SELECT AvaliacaoData AS DataCompleta FROM Avaliacao
        UNION ALL
        SELECT DataPagto AS DataCompleta FROM  UsrPagto
    ) AS d) AS cal
WHERE CAST(cal.DataCompleta AS DATE) NOT IN (SELECT DataCompleta FROM dw_alv.Calendario);

-- Adicionando dados ao fato "Avaliacao"
INSERT INTO dw_alv.Avaliacao
SELECT DISTINCT ON (a.AvaliacaoID)
    a.AvaliacaoID,
    dwg.GeneroKey,
    dwf.FilmeKey,
    dwp.ProdutoraKey,
    dwc.CalendarioKey,
    dwu.UsuarioKey,
    a.Nota,
    CAST(to_char(a.AvaliacaoData, 'HH24:MI:SSOF') AS TIME WITH TIME ZONE) AS HORA
FROM
    Avaliacao a INNER JOIN Filme f ON a.FilmeID = f.FilmeID
    INNER JOIN Filme_GeneroFilme g ON g.FilmeID = f.FilmeID
    INNER JOIN Produtora p ON p.ProdutoraID = f.ProdutoraID
    INNER JOIN Usuario u ON a.UsuarioID = u.UsuarioID
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
FROM
    dw_alv.Avaliacao;

-- Adicionando dados ao fato Receita
INSERT INTO dw_alv.Receita
SELECT DISTINCT ON (up.AssinaturaID)
    up.AssinaturaID,
    dwu.UsuarioKey,
    dwc.CalendarioKey,
    dwe.EnderecoKey,
    up.ValorPago AS Valor,
    CAST(to_char(up.DataPagto, 'HH24:MI:SSOF') AS TIME WITH TIME ZONE) AS Hora
FROM
    UsrPagto up INNER JOIN Usuario u ON up.UsuarioID = u.UsuarioID
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
