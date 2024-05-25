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
('F6', 'Comédia')
('F6', 'Drama')
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
('U4', 'Ass4', 10, '2023-11-24'),
('U5', 'Ass5', 30, '2024-05-25'),
('U6', 'Ass6', 40, '2023-02-28');

INSERT INTO Assiste (UsuarioID, FilmeID, Data) VALUES
('U4', 'F1', '2023-05-18'),
('U4', 'F4', '2024-01-29'),
('U5', 'F5', '2023-07-16'),
('U6', 'F6', '2023-09-02');

INSERT INTO Avaliacao (AvaliacaoID, Comentario, AvaliacaoData, Nota, UsuarioID, FilmeID, AssinaturaID) VALUES
('A4', 'Filme impecável com atuações maravilhosas. Me emocionei muito!', '2024-01-29', 10, 'U4', 'F4', 'Ass4'),
('A5', 'A intercalação entre passado, presente e futuro neste filme torna-o uma experiência única.', '2023-07-16', 9, 'U5', 'F5', 'Ass5'),
('A6', NULL, '2023-09-02', 8, 'U6', 'F6', 'Ass6');

INSERT INTO FilmPagtoRoy (FilmeID, ValorPagto, DataPagto) VALUES
('F4', 500, '2000-09-14'),
('F5', 700, '2009-01-23'),
('F6', 600, '2017-01-28');