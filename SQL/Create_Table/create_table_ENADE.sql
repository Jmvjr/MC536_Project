CREATE TABLE ENADE(
	id_enade INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	total_inscritos INT,
	total_concluintes INT,
	nota_bruta_ce REAL,
	nota_padronizada_ce REAL,
	nota_bruta_fg REAL,
	nota_padronizada_fg REAL,
	nota_enade_continua REAL,
	nota_enade_faixa REAL
);