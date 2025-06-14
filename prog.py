import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
# Correção da importação depreciada
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import psycopg2
import os
# Comentarios sobre mudanças a fazer no codigo:
# [] Dentro da definicao da classe Municipio existe um TO-DO.

# Remocoes:
# (1) Removi o atributo qtd_habitantes de UF. Não temos esse dado.
# (2) Removi os atributos derivados de Municipio para torna-los consultas SQL.
# (3) 

# Create base class for models
Base = declarative_base()

# Defina os caminhos para csv dos dados a serem populados
data_dir    = 'data/'
enade_2021  = os.path.join(data_dir, 'conceito_enade_2021.csv')
enade_2022  = os.path.join(data_dir, 'conceito_enade_2022.csv')
enade_2023  = os.path.join(data_dir, 'conceito_enade_2023.csv')
ideb        = os.path.join(data_dir, 'ideb_saeb_2017_2019_2021_2023.csv')
ID_GENERATOR = 0

# Criando as classes através da ORM
class UF(Base):
    def __str__(self):
        return f'id:{id}\n sigla:{self.sigla}\n nome:{self.nome}'

    __tablename__ = 'uf'
    
    id          = Column(Integer, primary_key=True, autoincrement=True)
    sigla    = Column(String(2), nullable=False, unique=True)
    nome        = Column(String(100), nullable=False, unique=True)
    
    uf_to_municipio  = relationship("Municipio", back_populates="municipio_to_uf")

class Municipio(Base):
    # TO-DO: Verificar se atributos derivados podem se tornar consultas SQL relevantes.
    __tablename__ = 'municipio'
    
    id      = Column(Integer, primary_key=True, autoincrement=True)
    id_uf   = Column(Integer, ForeignKey('uf.id'), nullable=False)
    nome    = Column(String(100), nullable=False)

    municipio_to_uf     = relationship("UF", back_populates="uf_to_municipio")
    municipio_to_escola = relationship("Escola", back_populates="escola_to_municipio")
    municipio_to_ies    = relationship("IES",    back_populates="ies_to_municipio")

    # TODAS ESSES ATRIBUTOS PODEM SER ADQUIRIDOS POR CONSULTAS SQL
    #qtd_IES = Column(Integer)
    #qtd_ESCOLAS = Column(Integer)
    #media_ideb = Column(Float)
    #media_saeb = Column(Float)
    #media_enade = Column(Float)
    #total_participantes = Column(Integer)

class Escola(Base):
    __tablename__ = 'escola'
    
    id           = Column(Integer, primary_key=True, autoincrement=True)
    id_municipio = Column(Integer, ForeignKey('municipio.id'), nullable=False)
    nome         = Column(String(100), nullable=False)
    codigo       = Column(Integer, unique=True, nullable=False)
    rede         = Column(String(100), nullable=False)
    
    escola_to_municipio = relationship("Municipio", back_populates="municipio_to_escola")
    escola_to_ideb      = relationship("IDEB", back_populates="ideb_to_escola")
    escola_to_saeb      = relationship("SAEB", back_populates="saeb_to_escola")
class IES(Base):
    __tablename__ = 'ies'
    
    id              = Column(Integer, primary_key=True, autoincrement=True)
    id_municipio    = Column(Integer, ForeignKey('municipio.id'), nullable=False)
    nome            = Column(String(100), nullable=False)
    sigla           = Column(String(20))
    codigo          = Column(Integer, nullable=False, unique=True)
    rede            = Column(String(100))
    
    ies_to_municipio   = relationship("Municipio", back_populates="municipio_to_ies")
    ies_to_curso       = relationship("Curso", back_populates="curso_to_ies")

class Curso(Base):
    __tablename__ = 'curso'
    
    id              = Column(Integer, primary_key=True, autoincrement=True)
    id_ies          = Column(Integer, ForeignKey('ies.id'), nullable=False)
    nome            = Column(String(100), nullable=False)
    
    curso_to_ies     = relationship("IES", back_populates="ies_to_curso")
    curso_to_enade   = relationship("ENADE", back_populates="enade_to_curso")

class IDEB(Base):
    __tablename__ = 'ideb'
    
    id          = Column(Integer, primary_key=True, autoincrement=True)
    id_escola   = Column(Integer, ForeignKey('escola.id'), nullable=False)
    ano         = Column(Integer, nullable=False)
    rend_1      = Column(Float)
    rend_2      = Column(Float)
    rend_3      = Column(Float)
    rend_4      = Column(Float)
    nota        = Column(Float)

    ideb_to_escola = relationship("Escola", back_populates="escola_to_ideb") 

class SAEB(Base):
    __tablename__ = 'saeb'
    
    id          = Column(Integer, primary_key=True, autoincrement=True)
    id_escola   = Column(Integer, ForeignKey('escola.id'), nullable=False)
    ano         = Column(Integer, nullable=False)
    nota_mat    = Column(Float)
    nota_port   = Column(Float)
    nota_padrao = Column(Float)
    
    saeb_to_escola = relationship("Escola", back_populates="escola_to_saeb")

class ENADE(Base):
    __tablename__ = 'enade'
    
    id                  = Column(Integer, primary_key=True, autoincrement=True)
    id_curso            = Column(Integer, ForeignKey('curso.id'), nullable=False)
    ano                 = Column(Integer, nullable=False)
    total_inscritos     = Column(Integer)
    total_concluintes   = Column(Integer)
    nota_bruta_ce       = Column(Float)
    nota_padronizada_ce = Column(Float)
    nota_bruta_fg       = Column(Float)
    nota_padronizada_fg = Column(Float)
    nota_enade_continua = Column(Float)
    nota_enade_faixa    = Column(Float)
    
    enade_to_curso = relationship("Curso", back_populates="curso_to_enade")
# Function to create the database and tables
'''
Funcao que cria as databases, necessita da connection_string para
realizar a conexao com o pgAdmin.

OBS:
- Dropa todas as tabelas ja definidas.
'''
def create_database():
    # Create engine
    engine = create_engine(connection_string)
    
    print("Removendo todas as tabelas existentes...")
    # Create all tables
    drop_all_with_cascade(engine)
    
    print("Criando novas tabelas...")
    Base.metadata.create_all(engine)    # Create tables
    
    print("Verificando se as tabelas foram criadas corretamente...")
    # Verificar se a tabela enade foi criada com as colunas corretas
    with engine.connect() as conn:
        result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'enade'"))
        columns = [row[0] for row in result]
        print(f"Colunas da tabela enade: {columns}")
        
        if 'id_curso' not in columns:
            print("ERRO: Coluna id_curso não encontrada na tabela enade!")
            raise Exception("Esquema da tabela enade incorreto")
    
    print("Tabelas criadas com sucesso!")
    return engine

def drop_all_with_cascade(engine):
    with engine.connect() as conn:
        conn.execute(
            text(
                '''
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
                '''
            )
        )
        conn.commit()

'''
Funcao que retorna o nome da UF a partir da sua sigla.
'''
def get_uf_full_name(sigla : str) -> str:
    if not isinstance(sigla, str):
        raise TypeError("sigla em get_uf_full_name(sigla) deve ser do tipo str")

    uf_dict = {
        'AC': 'Acre',
        'AL': 'Alagoas',
        'AP': 'Amapá',
        'AM': 'Amazonas',
        'BA': 'Bahia',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'ES': 'Espírito Santo',
        'GO': 'Goiás',
        'MA': 'Maranhão',
        'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais',
        'PA': 'Pará',
        'PB': 'Paraíba',
        'PR': 'Paraná',
        'PE': 'Pernambuco',
        'PI': 'Piauí',
        'RJ': 'Rio de Janeiro',
        'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul',
        'RO': 'Rondônia',
        'RR': 'Roraima',
        'SC': 'Santa Catarina',
        'SP': 'São Paulo',
        'SE': 'Sergipe',
        'TO': 'Tocantins'
    }
    
    key = sigla.strip().upper()
    try:
        return uf_dict[key]
    except KeyError:
        raise ValueError(f"Sigla não contida em uf_dict: {sigla}")

"""
Importa dados para a tabela UF

Args:
    engine: Conexão com banco de dados
    df_enade: DataFrame com dados do ENADE
    df_ideb: DataFrame com dados do IDEB
"""
def import_uf(engine, df_enade, df_ideb):
    print("Importando dados para a tabela UF...")
    
    # Combinar UFs dos dois DataFrames
    ufs_enade = df_enade['Sigla da UF'].drop_duplicates().tolist()
    ufs_ideb = df_ideb['UF'].drop_duplicates().tolist()
    
    # Unir as listas e eliminar duplicatas
    all_ufs = list(set(ufs_enade + ufs_ideb))
    
    # Criar DataFrame para UFs
    uf_df = pd.DataFrame({
        'id': range(1, len(all_ufs) + 1),
        'sigla_uf': all_ufs,
        'nome': [get_uf_full_name(uf) for uf in all_ufs],
    })
    
    # Criar sessão
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Inserir registros na tabela UF
        for _, row in uf_df.iterrows():
            uf = UF(
                id=row['id'],
                sigla=row['sigla_uf'],
                nome=row['nome'],
            )
            session.add(uf)
        
        session.commit()
        print(f"Importados {len(uf_df)} registros para a tabela UF.")
    except Exception as e:
        session.rollback()
        print(f"Erro ao importar dados para a tabela UF: {e}")
    finally:
        session.close()
    
    return uf_df


#function to import CSV data into the database
"""
    Importa dados dos arquivos CSV do ENADE e insere nas tabelas do PostgreSQL.
    Os dados lidos serão dos anos 2021, 2022 e 2023.
"""
def import_csv_enade(engine):
    # Cria uma sessão para interagir com o banco
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        print("Lendo arquivos CSV...")
        df2021 = pd.read_csv(enade_2021, encoding='latin1')
        df2022 = pd.read_csv(enade_2022, encoding='latin1')    
        df2023 = pd.read_csv(enade_2023,  encoding='latin1')
        df = pd.concat([df2021, df2022, df2023], ignore_index=True)
        
        print("Tratando dados...")
        # Tratando as tabelas.
        # Remove os cursos que tiveram avaliação 'SC' (Sem conceito).
        mask = df['Conceito Enade (Faixa)'] == 'SC'
        df = df[~mask]
        mask = df['Sigla da UF'].isna()
        df = df[~mask]
        mask = df[['Nota Bruta - FG', 'Nota Padronizada - FG', 'Nota Bruta - CE', 'Nota Padronizada - CE', 'Conceito Enade (Contínuo)', 'Conceito Enade (Faixa)', 'Sigla da IES']].isna().any(axis=1)
        df = df[~mask]
        df = df.reset_index(drop=True)
        
        # Dicionários para rastrear IDs
        # Utilizado na criação das chaves estrangeiras.
        uf_ids = {}
        municipio_ids = {}
        ies_ids = {}
        curso_ids = {}
        enade_ids = {}
        
        # 1. Inserir UFs
        print("Inserindo UFs...")
        for sigla_uf in df['Sigla da UF'].unique():
            # Verificar se UF já existe
            uf_obj = session.query(UF).filter_by(sigla=sigla_uf).first()
            if not uf_obj:
                uf_obj = UF(
                    nome=get_uf_full_name(sigla_uf),
                    sigla=sigla_uf
                )
                session.add(uf_obj)
                session.flush()
            uf_ids[sigla_uf] = uf_obj.id
        
        # 2. Inserir Municípios
        print("Inserindo Municípios...")
        municipios_df = df[['Município do Curso', 'Sigla da UF']].drop_duplicates().reset_index(drop=True)
        for _, row in municipios_df.iterrows():
            nome_municipio = row['Município do Curso']
            sigla_uf = row['Sigla da UF']
            
            # Chave composta para identificar o município
            municipio_key = f"{nome_municipio}_{sigla_uf}"
            
            # Verificar se já existe
            municipio_obj = session.query(Municipio).filter_by(
                nome=nome_municipio,
                id_uf=uf_ids[sigla_uf]
            ).first()
            
            if not municipio_obj:
                municipio_obj = Municipio(
                    id_uf=uf_ids[sigla_uf],
                    nome=nome_municipio,
                )
                session.add(municipio_obj)
                session.flush()
            
            municipio_ids[municipio_key] = municipio_obj.id
        
        # 3. Inserir IES
        print("Inserindo IES...")

        # Extrair as colunas relevantes
        ies_completo = df[['Código da IES', 'Nome da IES', 'Sigla da IES', 'Categoria Administrativa', 
                          'Município do Curso', 'Sigla da UF']]

        # Evitar SettingWithCopyWarning
        ies_completo = ies_completo.copy()
        # Criar uma chave composta para identificar combinações únicas de código IES e município
        ies_completo['chave_ies_municipio'] = ies_completo['Código da IES'].astype(str) + '_' + ies_completo['Município do Curso'] + '_' + ies_completo['Sigla da UF']

        # Manter apenas a primeira ocorrência de cada combinação única de código IES e município
        ies_df = ies_completo.drop_duplicates(subset=['chave_ies_municipio']).reset_index(drop=True)

        for _, row in ies_df.iterrows():
            cod_ies = int(row['Código da IES'])
            nome_ies = row['Nome da IES']
            sigla_ies = row['Sigla da IES']
            categoria = row['Categoria Administrativa']
            municipio_key = f"{row['Município do Curso']}_{row['Sigla da UF']}"
            
            if municipio_key in municipio_ids:
                # Verificar se a IES já existe com o mesmo código
                existing_ies = session.query(IES).filter_by(codigo=cod_ies).first()
                if not existing_ies:
                    ies_obj = IES(
                        id_municipio=municipio_ids[municipio_key],
                        nome=nome_ies,
                        sigla=sigla_ies,
                        codigo=cod_ies,
                        rede=categoria
                    )
                    session.add(ies_obj)
                    session.flush()
                    # Usar uma chave composta para rastrear IES por código e município
                    ies_ids[(cod_ies, municipio_key)] = ies_obj.id
                else:
                    # Se já existe, usar o ID existente
                    ies_ids[(cod_ies, municipio_key)] = existing_ies.id
            else:
                print(f"Chave não encontrada: {municipio_key}")
        
        # 4. Inserir Cursos
        print("Inserindo Cursos...")
        curso_df = df[['Código da IES', 'Área de Avaliação', 'Código do Curso', 'Município do Curso', 'Sigla da UF']].drop_duplicates().reset_index(drop=True)
        for _, row in curso_df.iterrows():
            cod_ies = int(row['Código da IES'])
            nome_curso = row['Área de Avaliação']
            cod_curso = int(row['Código do Curso'])
            municipio_key = f"{row['Município do Curso']}_{row['Sigla da UF']}"
            
            # Verificar se existe a combinação IES-município
            if (cod_ies, municipio_key) in ies_ids:
                # Verificar se o curso já existe
                curso_obj = session.query(Curso).filter_by(
                    id_ies=ies_ids[(cod_ies, municipio_key)],
                    nome=nome_curso
                ).first()
                
                if not curso_obj:
                    curso_obj = Curso(
                        id_ies=ies_ids[(cod_ies, municipio_key)],
                        nome=nome_curso
                    )
                    session.add(curso_obj)
                    session.flush()
                
                curso_ids[cod_curso] = curso_obj.id
        
        # 5. Inserir dados ENADE
        print("Inserindo dados ENADE...")
        for _, row in df.iterrows():
            try:
                cod_curso = int(row['Código do Curso'])
                ano_enade = int(row['Ano'])
                
                if cod_curso in curso_ids:
                    # Função para converter string com vírgula para float
                    def convert_to_float(value):
                        if isinstance(value, str):
                            # Substituir vírgula por ponto
                            return float(value.replace(',', '.'))
                        return float(value)
                    
                    # Dados do ENADE com conversão segura
                    enade_obj = ENADE(
                        id_curso=curso_ids[cod_curso],
                        ano=ano_enade,
                        total_inscritos=int(row['Nº de Concluintes Inscritos']),
                        total_concluintes=int(row['Nº  de Concluintes Participantes']),
                        nota_bruta_fg=convert_to_float(row['Nota Bruta - FG']),
                        nota_padronizada_fg=convert_to_float(row['Nota Padronizada - FG']),
                        nota_bruta_ce=convert_to_float(row['Nota Bruta - CE']),
                        nota_padronizada_ce=convert_to_float(row['Nota Padronizada - CE']),
                        nota_enade_continua=convert_to_float(row['Conceito Enade (Contínuo)']),
                        nota_enade_faixa=convert_to_float(row['Conceito Enade (Faixa)'])
                    )
                    session.add(enade_obj)
                    session.flush()
                    
                    enade_ids[(cod_curso, ano_enade)] = enade_obj.id
            except Exception as e:
                print(f"Erro ao processar linha com código de curso {row.get('Código do Curso', 'N/A')}: {e}")
                continue

        # Commit das alterações
        session.commit()
        print(f"Inserção concluída com sucesso! Inseridos:")
        print(f"- {len(uf_ids)} UFs")
        print(f"- {len(municipio_ids)} Municípios")
        print(f"- {len(ies_ids)} Instituições")
        print(f"- {len(curso_ids)} Cursos")
        print(f"- {len(enade_ids)} resultados ENADE")
        
    except Exception as e:
        # Em caso de erro, reverter alterações
        session.rollback()
        print(f"Erro durante a inserção: {str(e)}")
    finally:
        # Fechar a sessão
        session.close()
    
    
def import_csv_ideb(engine):
    """
    Importa dados de IDEB e SAEB do CSV e os insere nas tabelas do banco de dados.
    Os dados incluem informações para os anos 2017, 2019, 2021 e 2023.
    """
    # Criar sessão
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        print("Lendo arquivo CSV IDEB/SAEB...")
        df = pd.read_csv(ideb, encoding='latin1', low_memory=False)
        df = df.iloc[:-14]
        print(f"Total de registros lidos: {len(df)}")
        
        # Dicionários para rastrear IDs
        uf_ids = {}
        municipio_ids = {}
        escola_ids = {}
        ideb_ids = {}
        saeb_ids = {}
        
        # 1. Inserir/Obter UFs
        print("Processando UFs em import_csv_ideb...")
        for sigla_uf in df['Sigla da UF'].dropna().unique():
            # Verificar se é NaN
            if pd.isna(sigla_uf) or sigla_uf == 'nan':
                continue
            sigla_uf = str(sigla_uf).strip()
            # Verificar se UF já existe na sessão
            uf_obj = session.query(UF).filter_by(sigla=sigla_uf).first()
            if not uf_obj:
                uf_obj = UF(
                    sigla=sigla_uf,
                    nome=get_uf_full_name(sigla_uf)
                )
                session.add(uf_obj)
                session.flush()
            uf_ids[sigla_uf] = uf_obj.id
        
        # 2. Inserir/Obter Municípios
        print("Processando Municípios em import_csv_ideb...")
        for _, row in df[['Sigla da UF', 'Código do Município', 'Nome do Município']].drop_duplicates().iterrows():
            sigla_uf = row['Sigla da UF']
            nome_municipio = row['Nome do Município']
            cod_municipio = str(row['Código do Município'])
            
            # Chave composta para identificar o município
            municipio_key = f"{nome_municipio}_{sigla_uf}"
            
            # Verificar se já existe
            municipio_obj = session.query(Municipio).filter_by(
                nome=nome_municipio,
                id_uf=uf_ids[sigla_uf]
            ).first()
            
            if not municipio_obj:
                municipio_obj = Municipio(
                    id_uf=uf_ids[sigla_uf],
                    nome=nome_municipio,
                )
                session.add(municipio_obj)
                session.flush()
            
            municipio_ids[municipio_key] = municipio_obj.id
        
        # 3. Inserir Escolas
        print("Processando Escolas em import_csv_ideb...")
        for _, row in df[['Sigla da UF', 'Nome do Município', 'Código da Escola', 'Nome da Escola', 'Rede']].drop_duplicates().iterrows():
            codigo_escola = int(row['Código da Escola']) if pd.notna(row['Código da Escola']) else 0
            if codigo_escola == 0:
                continue  # Pular escolas sem código válido
                
            nome_escola = row['Nome da Escola']
            rede = row['Rede']
            sigla_uf = row['Sigla da UF']
            nome_municipio = row['Nome do Município']
            
            # Chave composta para identificar o município
            municipio_key = f"{nome_municipio}_{sigla_uf}"
            
            if municipio_key in municipio_ids:
                # Verificar se a escola já existe
                escola_obj = session.query(Escola).filter_by(codigo=codigo_escola).first()
                if not escola_obj:
                    escola_obj = Escola(
                        id_municipio=municipio_ids[municipio_key],
                        nome=nome_escola,
                        codigo=codigo_escola,
                        rede=rede
                    )
                    session.add(escola_obj)
                    session.flush()
                
                escola_ids[codigo_escola] = escola_obj.id
        
        # Adicione esta função de conversão
        def convert_to_float_or_none(value):
            """
            Converte um valor para float ou retorna None para valores "-".
            Também trata valores com vírgula como separador decimal.
            """
            if pd.isna(value) or value == '-':
                return None
            if isinstance(value, str):
                value = value.replace(',', '.')
            try:
                return float(value)
            except ValueError:
                return None
        
        # 4. Processar dados por ano (2017, 2019, 2021, 2023)
        anos = [2017, 2019, 2021, 2023]
        print("Processando dados por ano...")
        
        for _, row in df.iterrows():
            codigo_escola = int(row['Código da Escola']) if pd.notna(row['Código da Escola']) else 0
            if codigo_escola == 0 or codigo_escola not in escola_ids:
                continue
                
            for ano in anos:
                # Verificar se temos dados completos para este ano
                try:
                    # IDEB - usando convert_to_float_or_none para tratar valores "-"
                    rend_1 = convert_to_float_or_none(row[f'Taxa_Aprov_1serie_{ano}'])
                    rend_2 = convert_to_float_or_none(row[f'Taxa_Aprov_2serie_{ano}'])
                    rend_3 = convert_to_float_or_none(row[f'Taxa_Aprov_3serie_{ano}'])
                    rend_4 = convert_to_float_or_none(row[f'Taxa_Aprov_4serie_{ano}'])
                    nota_ideb = convert_to_float_or_none(row[f'Nota_ideb_{ano}'])
                    
                    # SAEB - usando convert_to_float_or_none para tratar valores "-" 
                    nota_mat = convert_to_float_or_none(row[f'Nota_SAEB_{ano}_Mat'])
                    nota_port = convert_to_float_or_none(row[f'Nota_SAEB_{ano}_Port'])
                    nota_padrao = convert_to_float_or_none(row[f'Nota_padronizada_SAEB_{ano}'])
                    
                    # Criar objetos apenas se temos pelo menos alguns dados
                    if any([nota_ideb is not None, nota_padrao is not None]):
                        # 4.1 Inserir IDEB
                        ideb_obj = IDEB(
                            id_escola=escola_ids[codigo_escola],
                            ano=ano,
                            rend_1=rend_1,
                            rend_2=rend_2,
                            rend_3=rend_3,
                            rend_4=rend_4,
                            nota=nota_ideb
                        )
                        session.add(ideb_obj)
                        session.flush()
                        
                        # 4.2 Inserir SAEB
                        saeb_obj = SAEB(
                            id_escola=escola_ids[codigo_escola],
                            ano=ano,
                            nota_mat=nota_mat,
                            nota_port=nota_port,
                            nota_padrao=nota_padrao
                        )
                        session.add(saeb_obj)
                        session.flush()
                        
                        # Guardar os IDs para referência futura
                        ideb_ids[(codigo_escola, ano)] = ideb_obj.id
                        saeb_ids[(codigo_escola, ano)] = saeb_obj.id
                    
                except Exception as e:
                    print(f"Erro ao processar dados do ano {ano} para escola {codigo_escola}: {e}")
                    continue
        
        ## Atualizar estatísticas dos municípios
        #print("Atualizando estatísticas dos municípios...")
        #for municipio_key, municipio_id in municipio_ids.items():
            ## Contagem de escolas
            #qtd_escolas = session.query(Escola).filter_by(id_municipio=municipio_id).count()
            
            ## Obter todas as escolas deste município
            #escolas_do_municipio = session.query(Escola.cod_escola).filter_by(id_municipio=municipio_id).all()
            #codigos_escolas = [e[0] for e in escolas_do_municipio]
            
            #ideb_valores = []
            #saeb_valores = []
            
            ## Para cada escola, buscar valores nos dicionários de IDs
            #for codigo_escola in codigos_escolas:
                #for ano in anos:
                    ## Verificar IDEB
                    #if (codigo_escola, ano) in ideb_ids:
                        #id_ideb = ideb_ids[(codigo_escola, ano)]
                        #ideb_obj = session.query(IDEB).filter_by(id_ideb=id_ideb).first()
                        #if ideb_obj and ideb_obj.nota_ideb is not None:
                            #ideb_valores.append(ideb_obj.nota_ideb)
                            
                    ## Verificar SAEB
                    #if (codigo_escola, ano) in saeb_ids:
                        #id_saeb = saeb_ids[(codigo_escola, ano)]
                        #saeb_obj = session.query(SAEB).filter_by(id_saeb=id_saeb).first()
                        #if saeb_obj and saeb_obj.nota_padrao is not None:
                            #saeb_valores.append(saeb_obj.nota_padrao)
            
            ## Calcular médias
            #media_ideb = sum(ideb_valores) / len(ideb_valores) if ideb_valores else 0.0
            #media_saeb = sum(saeb_valores) / len(saeb_valores) if saeb_valores else 0.0
            
            # Atualizar município
            #municipio = session.query(Municipio).filter_by(id_municipio=municipio_id).first()
            #if municipio:
                #municipio.qtd_ESCOLAS = qtd_escolas
                #municipio.media_ideb = media_ideb
                #municipio.media_saeb = media_saeb
        
        # Commit das alterações
        session.commit()
        print(f"Inserção concluída com sucesso! Inseridos:")
        print(f"- {len(escola_ids)} Escolas")
        print(f"- {len(ideb_ids)} registros IDEB")
        print(f"- {len(saeb_ids)} registros SAEB")
        
    except Exception as e:
        # Em caso de erro, reverter alterações
        session.rollback()
        print(f"Erro durante a inserção: {str(e)}")
    finally:
        # Fechar a sessão
        session.close()
    
'''
    Usado para criar os arquivos .txt de saída.
'''
def write_results_to_file(filename, header, rows):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        for row in rows:
            f.write(f"{row}\n")
    

# Formato da string de conexão: postgresql://username:password@host:port/database
# substitua por seu respectivo <user>, <password>, <host>, <port>, <database_name>
connection_string = "postgresql://postgres:1234@localhost:5432/mc536_project"

if __name__ == "__main__":
    criar_tabelas = True # Controle para não executar todo o programa

    if criar_tabelas:
        engine = create_database()
        import_csv_enade(engine)
        import_csv_ideb(engine)
        print("Criação das tabelas feita com sucesso!")
    else:
        engine = create_engine(connection_string)
    #df_ideb, saeb_df, ideb_df, municipios_df = import_csv_ideb(engine)
    #import_csv_ideb(engine)

    conn = psycopg2.connect(
        dbname="mc536_project",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    try:
        # 1. Média ENADE por UF e ano
        cur.execute("""
            SELECT 
                uf.nome AS nome_uf,
                e.ano,
                AVG(e.nota_enade_continua) AS media_nota_enade
            FROM enade e
            JOIN curso c ON e.id_curso = c.id
            JOIN ies i ON c.id_ies = i.id
            JOIN municipio m ON i.id_municipio = m.id
            JOIN uf ON m.id_uf = uf.id
            GROUP BY uf.nome, e.ano
            ORDER BY e.ano, media_nota_enade DESC;
        """)
        result_1 = cur.fetchall()
        write_results_to_file(
            'media_enade_up_ano.txt',
            '------- MÉDIA ENADE POR UF E ANO --------',
            result_1
        ) 

        # 2. Top 5 notas SAEB 2023
        cur.execute("""
            SELECT 
                s.ano,
                s.nota_mat,
                s.nota_port,
                (s.nota_mat + s.nota_port)/2 AS media
            FROM saeb AS s
            WHERE s.ano = 2023
                AND s.nota_mat IS NOT NULL
                AND s.nota_port IS NOT NULL
            ORDER BY media DESC
            LIMIT 5;
        """)
        result_2 = cur.fetchall()
        write_results_to_file(
            "top5_notas_saeb_2023.txt",
            "TOP 5 SAEB - 2023",
            result_2
        )

        # 3. Notas dos cursos avaliados pelo enade da unicamp
        cur.execute("""
        SELECT 
            i.nome AS nome_ies,
            c.nome AS nome_curso,
            e.ano,
            e.nota_enade_continua
        FROM enade e
            JOIN curso c ON e.id_curso = c.id
            JOIN ies i ON i.id = c.id_ies
        WHERE 
            i.nome ILIKE '%campinas%'
            AND i.nome ILIKE '%estadual%'
        GROUP BY c.nome, e.ano, e.nota_enade_continua, i.nome
        ORDER BY i.nome, c.nome, e.ano;
        """)
        result_3 = cur.fetchall()
        write_results_to_file(
            "notas_da_unicamp_enade.txt",
            "notas unicamp dentro do enade".strip().upper(),
            result_3
        )

        # 4. Média SAEB por município - this query needs to be recalculated from the data
        cur.execute("""
            SELECT 
                m.nome AS nome_municipio,
                AVG(s.nota_padrao) AS media_saeb
            FROM municipio m
            JOIN escola e ON e.id_municipio = m.id
            JOIN saeb s ON s.id_escola = e.id
            WHERE s.nota_padrao IS NOT NULL
            GROUP BY m.nome
            ORDER BY media_saeb DESC;
        """)
        result_4 = cur.fetchall()
        write_results_to_file(
            "media_saeb_municipio.txt",
            "media saeb por municipio".strip().upper(),
            result_4
        )

        # 5. Número de escolas por rede e UF
        cur.execute("""
            SELECT 
                uf.nome AS nome_uf,
                e.rede,
                COUNT(*) AS total_escolas
            FROM escola e
            JOIN municipio m ON e.id_municipio = m.id
            JOIN uf ON m.id_uf = uf.id
            GROUP BY uf.nome, e.rede
            ORDER BY uf.nome, total_escolas DESC;
        """)
        result_5 = cur.fetchall()
        write_results_to_file(
            "numero_escolas_rede_uf.txt",
            "Número de escolas por rede e UF".strip().upper(),
            result_5
        )

        # 6. Cursos de ciência da computação com maior conceito enade
        cur.execute(
            '''
            SELECT 
                ies.nome AS nome_ies,
                curso.nome AS nome_curso,
                AVG(enade.nota_enade_continua) AS media_enade
            FROM enade
                JOIN curso ON enade.id_curso = curso.id
                JOIN ies ON ies.id = curso.id_ies
            WHERE  
                curso.id IS NOT NULL 
                AND ies.id IS NOT NULL
                AND enade.id IS NOT NULL
                AND curso.nome ILIKE '%computação%'
            GROUP BY
                ies.nome, curso.nome
            ORDER BY
                media_enade DESC;
            '''
        )
        result_6 = cur.fetchall()
        write_results_to_file(
            "melhores_escola_comp.txt",
            "melhores escolas de computação".strip().upper(),
            result_6
        )

        #cur.close()
        #conn.close()
        #print("Conexão encerrada com sucesso.")
    finally:
        cur.close()
        conn.close()
        print("Conexão encerrada com sucesso.")
