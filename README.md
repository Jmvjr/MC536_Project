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
* **Bibliotecas Python: pandas, sqlalchemy**
