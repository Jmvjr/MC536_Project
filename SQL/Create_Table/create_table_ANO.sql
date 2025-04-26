CREATE TABLE ANO(
	id_ano INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id_curso INT REFERENCES curso(id_curso),
	id_enade INT REFERENCES enade(id_enade),
	id_saeb INT REFERENCES saeb(id_saeb),
	id_ideb INT REFERENCES ideb(id_ideb),
	ano INT NOT NULL
);