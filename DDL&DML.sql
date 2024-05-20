CREATE TABLE Produtora (
    ProdutoraID VARCHAR(2) PRIMARY KEY,
    ProdutoraNome VARCHAR(100) NOT NULL
);

CREATE TABLE Filme (
    FilmeID VARCHAR(2) PRIMARY KEY,
    DuracaoMin INT,
    FilmeNome VARCHAR(100),
    AnoDeLancamento INT,
    ProdutoraID VARCHAR(2),
    FOREIGN KEY (ProdutoraID) REFERENCES Produtora(ProdutoraID)
);

CREATE TABLE Plano (
    PlanoID VARCHAR(3) PRIMARY KEY,
    PrecoMensal DECIMAL(10, 2),
    PlanoNome VARCHAR(50)
);

CREATE TABLE Usuario (
    UsuarioID VARCHAR(2) PRIMARY KEY,
    Email VARCHAR(100),
    Telefone VARCHAR(15),
    DataVencimento DATE,
    CodigoDeSeguranca INT,
    NumeroDoCartao VARCHAR(16),
    NomeDoProprietario VARCHAR(100),
    Senha VARCHAR(50),
    UsuarioNome VARCHAR(100),
    Bairro VARCHAR(100),
    Municipio VARCHAR(100),
    Estado VARCHAR(2),
    Logradouro VARCHAR(100)
);

CREATE TABLE Filme_GeneroFilme (
    FilmeID VARCHAR(2),
    GeneroFilme VARCHAR(50),
    PRIMARY KEY (FilmeID, GeneroFilme),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);

CREATE TABLE Filme_DiretorFilme (
    FilmeID VARCHAR(2),
    DiretorFilme VARCHAR(100),
    PRIMARY KEY (FilmeID, DiretorFilme),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);

CREATE TABLE Filme_AtorFilme (
    FilmeID VARCHAR(2),
    AtorFilme VARCHAR(100),
    PRIMARY KEY (FilmeID, AtorFilme),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);

CREATE TABLE Assinatura (
    AssinaturaID VARCHAR(4) PRIMARY KEY,
    DataInicio DATE,
    DataFim DATE,
    Status VARCHAR(50),
    PlanoID VARCHAR(3),
    FOREIGN KEY (PlanoID) REFERENCES Plano(PlanoID)
);

CREATE TABLE UsrPagto (
    UsuarioID VARCHAR(2),
    AssinaturaID VARCHAR(4),
    ValorPago DECIMAL(10, 2),
    DataPagto DATE,
    PRIMARY KEY (UsuarioID, AssinaturaID),
    FOREIGN KEY (UsuarioID) REFERENCES Usuario(UsuarioID),
    FOREIGN KEY (AssinaturaID) REFERENCES Assinatura(AssinaturaID)
);

CREATE TABLE Assiste (
    UsuarioID VARCHAR(2),
    FilmeID VARCHAR(2),
    Data DATE,
    PRIMARY KEY (UsuarioID, FilmeID, Data),
    FOREIGN KEY (UsuarioID) REFERENCES Usuario(UsuarioID),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);

CREATE TABLE Avaliacao (
    AvaliacaoID VARCHAR(2) PRIMARY KEY,
    Comentario TEXT,
    AvaliacaoData DATE,
    Nota INT,
    UsuarioID VARCHAR(2),
    FilmeID VARCHAR(2),
    AssinaturaID VARCHAR(4),
    FOREIGN KEY (UsuarioID) REFERENCES Usuario(UsuarioID),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID),
    FOREIGN KEY (AssinaturaID) REFERENCES Assinatura(AssinaturaID)
);

CREATE TABLE Cargo (
    CargoID VARCHAR(2) PRIMARY KEY,
    CargoNome VARCHAR(100)
);

CREATE TABLE Funcionario (
    FuncionarioID VARCHAR(5) PRIMARY KEY,
    Salario DECIMAL(10, 2),
    FuncionarioNome VARCHAR(100),
    CargoID VARCHAR(2),
    FOREIGN KEY (CargoID) REFERENCES Cargo(CargoID)
);

CREATE TABLE Modera (
    FuncionarioID VARCHAR(5),
    AvaliacaoID VARCHAR(2),
    PRIMARY KEY (FuncionarioID, AvaliacaoID),
    FOREIGN KEY (FuncionarioID) REFERENCES Funcionario(FuncionarioID),
    FOREIGN KEY (AvaliacaoID) REFERENCES Avaliacao(AvaliacaoID)
);

CREATE TABLE GerenciaConteudo (
    FuncionarioID VARCHAR(5),
    FilmeID VARCHAR(2),
    PRIMARY KEY (FuncionarioID, FilmeID),
    FOREIGN KEY (FuncionarioID) REFERENCES Funcionario(FuncionarioID),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);

CREATE TABLE FilmPagtoRoy (
    FilmeID VARCHAR(2),
    ValorPagto DECIMAL(10, 2),
    DataPagto DATE,
    PRIMARY KEY (FilmeID, DataPagto),
    FOREIGN KEY (FilmeID) REFERENCES Filme(FilmeID)
);


-- Inserindo dados na tabela Produtora
INSERT INTO Produtora (ProdutoraID, ProdutoraNome) VALUES
('P1', 'Warner Bros. Pictures'),
('P2', 'Paramount Pictures'),
('P3', 'Universal Pictures');

-- Inserindo dados na tabela Filme
INSERT INTO Filme (FilmeID, DuracaoMin, FilmeNome, AnoDeLancamento, ProdutoraID) VALUES
('F1', 152, 'Harry Potter e a Pedra Filosofal', 2001, 'P1'),
('F2', 169, 'Interestelar', 2014, 'P2'),
('F3', 127, 'Jurassic Park', 1993, 'P3');

-- Inserindo dados na tabela Plano
INSERT INTO Plano (PlanoID, PrecoMensal, PlanoNome) VALUES
('Pl1', 10, 'Light'),
('Pl2', 30, 'Premium'),
('Pl3', 40, 'Familia');

-- Inserindo dados na tabela Usuario
INSERT INTO Usuario (UsuarioID, Email, Telefone, DataVencimento, CodigoDeSeguranca, NumeroDoCartao, NomeDoProprietario, Senha, UsuarioNome, Bairro, Municipio, Estado, Logradouro) VALUES
('U1', 'joao.silva@example.com', '11987654321', '2023-06-15', 123, '4111111111111111', 'João Silva', 'password123', 'João Silva', 'Jardim Paulista', 'São Paulo', 'SP', 'Rua das Flores 123'),
('U2', 'maria.souza@example.com', '21987654321', '2024-07-20', 456, '4222222222222222', 'Maria Souza', 'password456', 'Maria Souza', 'Copacabana', 'Rio de Janeiro', 'RJ', 'Avenida Atlântica 456'),
('U3', 'carlos.pereira@example.com', '31987654321', '2024-08-25', 789, '4333333333333333', 'Carlos Pereira', 'password789', 'Carlos Pereira', 'Centro', 'Belo Horizonte', 'MG', 'Rua da Bahia 789');

-- Inserindo dados na tabela Filme_GeneroFilme
INSERT INTO Filme_GeneroFilme (FilmeID, GeneroFilme) VALUES
('F1', 'Fantasia'),
('F1', 'Aventura'),
('F2', 'Ficção científica'),
('F2', 'Drama'),
('F3', 'Ação'),
('F3', 'Aventura'),
('F3', 'Ficção científica');

-- Inserindo dados na tabela Filme_DiretorFilme
INSERT INTO Filme_DiretorFilme (FilmeID, DiretorFilme) VALUES
('F1', 'Chris Columbus'),
('F2', 'Christopher Nolan'),
('F3', 'Steven Spielberg');

-- Inserindo dados na tabela Filme_AtorFilme
INSERT INTO Filme_AtorFilme (FilmeID, AtorFilme) VALUES
('F1', 'Daniel Radcliffe'),
('F1', 'Emma Watson'),
('F2', 'Matthew McConaughey'),
('F2', 'Anne Hathaway'),
('F3', 'Sam Neill'),
('F3', 'Laura Dern');

-- Inserindo dados na tabela Assinatura
INSERT INTO Assinatura (AssinaturaID, DataInicio, DataFim, Status, PlanoID) VALUES
('Ass1', '2023-09-16', '2024-09-16', 'Ativo', 'Pl1'),
('Ass2', '2023-04-02', '2024-04-02', 'Desativado', 'Pl2'),
('Ass3', '2024-01-30', '2025-01-30', 'Ativo', 'Pl3');

-- Inserindo dados na tabela UsrPagto
INSERT INTO UsrPagto (UsuarioID, AssinaturaID, ValorPago, DataPagto) VALUES
('U1', 'Ass1', 10, '2023-11-16'),
('U2', 'Ass2', 30, '2023-07-02'),
('U3', 'Ass3', 40, '2024-02-28');

-- Inserindo dados na tabela Assiste
INSERT INTO Assiste (UsuarioID, FilmeID, Data) VALUES
('U1', 'F1', '2023-04-17'),
('U2', 'F1', '2023-12-28'),
('U2', 'F2', '2023-06-15'),
('U3', 'F3', '2023-08-01');

-- Inserindo dados na tabela Avaliacao
INSERT INTO Avaliacao (AvaliacaoID, Comentario, AvaliacaoData, Nota, UsuarioID, FilmeID, AssinaturaID) VALUES
('A1', 'Uma introdução mágica e encantadora ao mundo de Hogwarts, perfeita para todas as idades.', '2023-04-17', 5, 'U1', 'F1', 'Ass1'),
('A2', 'Uma épica jornada de ficção científica que combina emoção e ciência de forma brilhante.', '2023-08-01', 6, 'U2', 'F2', 'Ass2'),
('A3', NULL, '2023-08-02', 8, 'U3', 'F3', 'Ass3');

-- Inserindo dados na tabela Cargo
INSERT INTO Cargo (CargoID, CargoNome) VALUES
('C1', 'Gerente de conteúdo'),
('C2', 'Engenheiro de qualidade de streaming');

-- Inserindo dados na tabela Funcionario
INSERT INTO Funcionario (FuncionarioID, Salario, FuncionarioNome, CargoID) VALUES
('Func1', 4500, 'Ana Silva', 'C1'),
('Func2', 5200, 'Marcos Oliveira', 'C1'),
('Func3', 4800, 'Juliana Santos', 'C2');

-- Inserindo dados na tabela Modera
INSERT INTO Modera (FuncionarioID, AvaliacaoID) VALUES
('Func1', 'A1'),
('Func2', 'A2');

-- Inserindo dados na tabela GerenciaConteudo
INSERT INTO GerenciaConteudo (FuncionarioID, FilmeID) VALUES
('Func1', 'F1'),
('Func1', 'F2'),
('Func2', 'F2');

-- Inserindo dados na tabela FilmPagtoRoy
INSERT INTO FilmPagtoRoy (FilmeID, ValorPagto, DataPagto) VALUES
('F1', 500, '2001-03-30'),
('F2', 700, '2014-06-30'),
('F3', 600, '1993-09-30');