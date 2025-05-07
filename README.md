# Projeto de Integração de Dados Educacionais

Este projeto integra dados do ENADE, IDEB e SAEB em um banco de dados relacional PostgreSQL, permitindo análises sobre a qualidade da educação brasileira em diferentes níveis de ensino.

### Passos para executar o projeto:

#### 1: Crie um banco de dados
Certifique-se de que você tem um banco de dados PostgreSQL disponível. Se necessário, crie um novo banco com o comando:

```sql
CREATE DATABASE mc536;
```

#### 2: Caso esteja usando Docker
Se estiver utilizando Docker para hospedar o PostgreSQL, inicie o container:
```
docker start nome-do-container-postgres
```

#### 3: Configure a conexão com o banco de dados
Altere a variável connection_string na função create_database() substituindo com as informações do seu banco de dados:
```
connection_string = "postgresql://username:password@host:port/database"
```

#### 4: Execute o programa
```
python3 prog.py
```
## O programa irá:
1. **Criar as tabelas necessárias**
2. **Importar os dados dos arquivos CSV na pasta data**
3. **Processar e relacionar as informações**
4. **Calcular estatísticas básicas por município**

# Projeto de Integração de Dados Educacionais

Este projeto integra dados do ENADE, IDEB e SAEB em um banco de dados relacional PostgreSQL, permitindo análises sobre a qualidade da educação brasileira em diferentes níveis de ensino.

### Passos para executar o projeto:

#### 1: Crie um banco de dados
Certifique-se de que você tem um banco de dados PostgreSQL disponível. Crie um banco de dados e defina as propriedades:
- database_name
- user
- password
- host
- port

Dentro de prog.py você deve alterar a conexão do psycopg2 que é definida logo após a execução da main do programa:

'''python
    conn = psycopg2.connect(
        dbname="<database_name>",
        user="<user>",
        password="<password>",
        host="<host>",
        port="<port>"
    )
'''

Além disso, você deve alterar a string de conexão na função create_database() 
'''python
connection_string = "postgresql://<user>:<password>@<host>:<port>/<database_name>"
'''

#### 2: Execute o programa
```
python3 prog.py
```

Haverá o tempo de execução de população das tabelas.
Após o final da execução teremos os resultados gravados em:

**media_enade_up_ano.txt**
para a consulta:
'''sql
 SELECT 
    uf.nome AS nome_uf,
    a.ano,
    AVG(e.nota_enade_continua) AS media_nota_enade
FROM enade e
JOIN ano a ON e.id_enade = a.id_enade
JOIN curso c ON a.id_curso = c.id_curso
JOIN ies i ON c.id_ies = i.id_ies
JOIN municipio m ON i.id_municipio = m.id_municipio
JOIN uf ON m.id_uf = uf.id
GROUP BY uf.nome, a.ano
ORDER BY a.ano, media_nota_enade DESC;
'''

**top5_notas_saeb_2023.txt**
para a consulta:
'''sql
SELECT 
    a.ano,
    s.nota_mat,
    s.nota_port
FROM saeb s
JOIN ano a ON s.id_saeb = a.id_saeb
WHERE a.ano = 2023
ORDER BY (s.nota_mat + s.nota_port)/2 DESC
LIMIT 5;
'''

**evolucao_ideb_por_curso.txt**
para a consulta:
'''sql
SELECT 
    c.nome_curso,
    a.ano,
    AVG(i.nota_ideb) AS media_ideb
FROM ideb i
JOIN ano a ON i.id_ideb = a.id_ideb
JOIN curso c ON a.id_curso = c.id_curso
GROUP BY c.nome_curso, a.ano
ORDER BY c.nome_curso, a.ano;
'''

**media_saeb_municipio.txt**
para a consulta:
'''sql
SELECT 
    m.nome_municipio,
    m.media_saeb
FROM municipio m
ORDER BY m.media_saeb DESC;
'''

**numero_escolas_rede_uf.txt**
para a consulta:
'''sql
SELECT 
    uf.nome AS nome_uf,
    e.rede,
    COUNT(*) AS total_escolas
FROM escola e
JOIN municipio m ON e.id_municipio = m.id_municipio
JOIN uf ON m.id_uf = uf.id
GROUP BY uf.nome, e.rede
ORDER BY uf.nome, total_escolas DESC;
'''

## O programa irá:

1. **Criar as tabelas necessárias**
2. **Importar os dados dos arquivos CSV na pasta data**
3. **Processar e relacionar as informações**
4. **Calcular estatísticas básicas por município**

## Estrutura de dados:

* **UF: unidades federativas**
* **Município: municípios brasileiros**
* **IES: instituições de ensino superior**
* **Curso: cursos superiores**
* **Escola: escolas de ensino básico**
* **ENADE: resultados do Exame Nacional de Desempenho de Estudantes**
* **IDEB: resultados do Índice de Desenvolvimento da Educação Básica**
* **SAEB: resultados do Sistema de Avaliação da Educação Básica**
* **Ano: relaciona as entidades com seus respectivos anos de avaliação**


## Requisitos

* **Python 3.6+**
* **PostgreSQL**
* **Bibliotecas Python: pandas, sqlalchemy, psycopg2**
