-- Comandos sql no duckdb para extrair os dados de avaliações.
-- Juntando tabelas.
SELECT title as titulo, r.averageRating as avaliacao
FROM read_csv('title.akas.tsv', delim='\t', quote='', all_varchar=true, header=true, AUTO_DETECT=TRUE) t
INNER JOIN read_csv('title.ratings.tsv', delim='\t', quote='', all_varchar=true, header=true, AUTO_DETECT=TRUE) r ON t.titleId=r.tconst
WHERE t.region='BR' 
and (t.title = 'Interestelar'
or t.title = 'Harry Potter e a Pedra Filosofal'
or t.title = 'Jurassic Park - O Parque dos Dinossauros'
or t.title = 'Central do Brasil'
or t.title = 'Estômago'
or t.title = 'Que Horas Ela Volta?');

-- Criando CSV
COPY (
    SELECT title as titulo, r.averageRating as avaliacao
    FROM read_csv('title.akas.tsv', delim='\t', quote='', all_varchar=true, header=true, AUTO_DETECT=TRUE) t
    INNER JOIN read_csv('title.ratings.tsv', delim='\t', quote='', all_varchar=true, header=true, AUTO_DETECT=TRUE) r ON t.titleId=r.tconst
    WHERE t.region='BR' 
    and (t.title = 'Interestelar'
    or t.title = 'Harry Potter e a Pedra Filosofal'
    or t.title = 'Jurassic Park - O Parque dos Dinossauros'
    or t.title = 'Central do Brasil'
    or t.title = 'Estômago'
    or t.title = 'Que Horas Ela Volta?')
) TO 'avaliacoes.csv' (HEADER, DELIMITER ',');

-- Agora no MOBA

-- Criando tabela para colocar os dados extraídos.
CREATE TABLE avaliacoes_IMDb(
    FilmeNome VARCHAR(200) NOT NULL,
    Nota REAL NOT NULL
);