Drop schema if exists dw_alv cascade;
create schema dw_alv;

set search_path=dw_alv;


CREATE TABLE Endereco
(
  EnderecoKey VARCHAR NOT NULL,
  Estado VARCHAR NOT NULL,
  Municipio VARCHAR NOT NULL,
  Bairro VARCHAR NOT NULL,
  Logradouro VARCHAR NOT NULL,
  PRIMARY KEY (EnderecoKey)
);

CREATE TABLE Calendario
(
  CalendarioKey VARCHAR NOT NULL,
  DataCompleta DATE NOT NULL,
  DiaDaSemana VARCHAR NOT NULL,
  Dia INT NOT NULL,
  Mes VARCHAR NOT NULL,
  Trimestre INT NOT NULL,
  Ano INT NOT NULL,
  PRIMARY KEY (CalendarioKey)
);

CREATE TABLE Usuario
(
  UsuarioKey VARCHAR NOT NULL,
  UsuarioID VARCHAR NOT NULL,
  Email VARCHAR NOT NULL,
  Telefone INT NOT NULL,
  DataVencimento DATE NOT NULL,
  CodigoDeSegurança INT NOT NULL,
  NumeroDoCartão INT NOT NULL,
  NomeDoProprietario VARCHAR NOT NULL,
  Senha VARCHAR NOT NULL,
  UsuarioNome VARCHAR NOT NULL,
  PRIMARY KEY (UsuarioKey)
);

CREATE TABLE Genero
(
  GeneroKey VARCHAR NOT NULL,
  GeneroNome VARCHAR NOT NULL,
  PRIMARY KEY (GeneroKey)
);

CREATE TABLE Filme 
(
    FilmeKey VARCHAR NOT NULL,
    FilmeID VARCHAR NOT NULL,
    DuracaoMin INT NOT NULL,
	FilmeNome VARCHAR NOT NULL,
	AnoDeLancamento INT NOT NULL,
    PRIMARY KEY (FilmeKey)
);

CREATE TABLE Produtora
(
  ProdutoraKey VARCHAR NOT NULL,
  ProdutoraID VARCHAR NOT NULL,
  ProdutoraNome VARCHAR NOT NULL,
  PRIMARY KEY (GeneroKey)
);

CREATE TABLE Avaliações
(
  AvaliacaoID VARCHAR NOT NULL,
  GeneroKey VARCHAR NOT NULL,
  FilmeKey VARCHAR NOT NULL,
  ProdutoraKey VARCHAR NOT NULL,
  CalendarioKey VARCHAR NOT NULL,
  Nota INT NOT NULL,
  Hora TIME NOT NULL,
  PRIMARY KEY (AvaliacaoID, GeneroKey),
  FOREIGN KEY (GeneroKey) REFERENCES Genero(GeneroKey),
  FOREIGN KEY (FilmeKey) REFERENCES Filme(FilmeKey),
  FOREIGN KEY (ProdutoraKey) REFERENCES Produtora(ProdutoraKey),
  FOREIGN KEY (CalendarioKey) REFERENCES Calendario(CalendarioKey)
);

CREATE TABLE Receita
(
  AssinaturaID VARCHAR NOT NULL,
  UsuarioKey VARCHAR NOT NULL,
  CalendarioKey VARCHAR NOT NULL,
  EnderecoKey VARCHAR NOT NULL,
  Valor INT NOT NULL,
  Hora TIME NOT NULL,
  PRIMARY KEY (AssinaturaID),
  FOREIGN KEY (UsuarioKey) REFERENCES Usuario(UsuarioKey),
  FOREIGN KEY (EnderecoKey) REFERENCES Endereco(EnderecoKey),
  FOREIGN KEY (CalendarioKey) REFERENCES Calendario(CalendarioKey)
);


