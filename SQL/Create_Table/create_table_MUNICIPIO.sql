CREATE TABLE MUNICIPIO(
	id_municipio INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id_uf INT REFERENCES UF (id),
	nome_municipio VARCHAR(100) NOT NULL,
	qtd_IES INT,
	qtd_ESCOLAS INT,
	media_ideb REAL,
	media_saeb REAL,
	media_enade REAL,
	total_participantes INT
);
