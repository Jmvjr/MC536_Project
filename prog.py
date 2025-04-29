import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import os

# Create base class for models
Base = declarative_base()
data_dir = 'data/'
enade_2021 = os.path.join(data_dir, 'conceito_enade_2021.csv')
enade_2022 = os.path.join(data_dir, 'conceito_enade_2022.csv')
enade_2023 = os.path.join(data_dir, 'conceito_enade_2023.csv')
ideb = os.path.join(data_dir, 'ideb_saeb_2017_2019_2021_2023.csv')
# Define models corresponding to tables
class UF(Base):
    __tablename__ = 'uf'
    
    id = Column(Integer, primary_key=True)
    sigla_uf = Column(String(2))
    nome = Column(String(100))
    qtd_habitantes = Column(Integer)
    
    municipios = relationship("Municipio", back_populates="uf")

class Municipio(Base):
    __tablename__ = 'municipio'
    
    id_municipio = Column(Integer, primary_key=True)
    id_uf = Column(Integer, ForeignKey('uf.id'))
    nome_municipio = Column(String(100), nullable=False)
    qtd_IES = Column(Integer)
    qtd_ESCOLAS = Column(Integer)
    media_ideb = Column(Float)
    media_saeb = Column(Float)
    media_enade = Column(Float)
    total_participantes = Column(Integer)
    
    uf = relationship("UF", back_populates="municipios")
    escolas = relationship("Escola", back_populates="municipio")
    ies_list = relationship("IES", back_populates="municipio")

class IDEB(Base):
    __tablename__ = 'ideb'
    
    id_ideb = Column(Integer, primary_key=True)
    rend_1 = Column(Float)
    rend_2 = Column(Float)
    rend_3 = Column(Float)
    rend_4 = Column(Float)
    ind_rend = Column(Float)
    nota_ideb = Column(Float)
    
    anos = relationship("Ano", back_populates="ideb")

class ENADE(Base):
    __tablename__ = 'enade'
    
    id_enade = Column(Integer, primary_key=True)
    total_inscritos = Column(Integer)
    total_concluintes = Column(Integer)
    nota_bruta_ce = Column(Float)
    nota_padronizada_ce = Column(Float)
    nota_bruta_fg = Column(Float)
    nota_padronizada_fg = Column(Float)
    nota_enade_continua = Column(Float)
    nota_enade_faixa = Column(Float)
    
    anos = relationship("Ano", back_populates="enade")

class SAEB(Base):
    __tablename__ = 'saeb'
    
    id_saeb = Column(Integer, primary_key=True)
    nota_mat = Column(Float)
    nota_port = Column(Float)
    nota_padrao = Column(Float)
    
    anos = relationship("Ano", back_populates="saeb")

class IES(Base):
    __tablename__ = 'ies'
    
    id_ies = Column(Integer, primary_key=True)
    id_municipio = Column(Integer, ForeignKey('municipio.id_municipio'))
    nome_ies = Column(String(100), nullable=False)
    sigla_ies = Column(String(20))
    codigo_ies = Column(Integer, unique=True, nullable=False)
    rede = Column(String(100), nullable=False)
    
    municipio = relationship("Municipio", back_populates="ies_list")
    cursos = relationship("Curso", back_populates="ies")

class Escola(Base):
    __tablename__ = 'escola'
    
    id_escola = Column(Integer, primary_key=True)
    id_municipio = Column(Integer, ForeignKey('municipio.id_municipio'))
    nome_escola = Column(String(100), nullable=False)
    cod_escola = Column(Integer, unique=True, nullable=False)
    rede = Column(String(100), nullable=False)
    
    municipio = relationship("Municipio", back_populates="escolas")

class Curso(Base):
    __tablename__ = 'curso'
    
    id_curso = Column(Integer, primary_key=True)
    id_IES = Column(Integer, ForeignKey('ies.id_ies'))
    nome_curso = Column(String(100), nullable=False)
    
    ies = relationship("IES", back_populates="cursos")
    anos = relationship("Ano", back_populates="curso")

class Ano(Base):
    __tablename__ = 'ano'
    
    id_ano = Column(Integer, primary_key=True)
    id_curso = Column(Integer, ForeignKey('curso.id_curso'))
    id_enade = Column(Integer, ForeignKey('enade.id_enade'))
    id_saeb = Column(Integer, ForeignKey('saeb.id_saeb'))
    id_ideb = Column(Integer, ForeignKey('ideb.id_ideb'))
    ano = Column(Integer, nullable=False)
    
    curso = relationship("Curso", back_populates="anos")
    enade = relationship("ENADE", back_populates="anos")
    saeb = relationship("SAEB", back_populates="anos")
    ideb = relationship("IDEB", back_populates="anos")

# Function to create the database and tables
def create_database():
    # Connection string format: postgresql://username:password@host:port/database
    # Replace with your actual PostgreSQL connection details
    connection_string = "postgresql://mauricio:test@localhost:5432/mc536"
    
    # Create engine
    engine = create_engine(connection_string)
    
    # Create all tables
    Base.metadata.drop_all(engine)  # Drop tables if they exist
    Base.metadata.create_all(engine)  # Create tables
    
    return engine


def import_ANO():


#function to import CSV data into the database
def import_csv_enade(engine):
    df2021 = pd.read_csv(enade_2021, encoding='latin1')
    df2022 = pd.read_csv(enade_2022, encoding='latin1')    
    df2023 = pd.read_csv(enade_2023,  encoding='latin1')
    df = pd.concat([df2021, df2022, df2023])
    
    #tratando tabelas
    mask = df['Conceito Enade (Faixa)'] == 'SC'
    df = df[~mask]
    mask = df['Sigla da UF'].isna()
    df = df[~mask]
    mask = df[['Nota Bruta - FG', 'Nota Padronizada - FG', 'Nota Bruta - CE', 'Nota Padronizada - CE', 'Conceito Enade (Contínuo)', 'Conceito Enade (Faixa)', 'Sigla da IES']].isna().any(axis=1)
    df = df[~mask]
    df = df.reset_index(drop=True)
    
    # Separando tabelas
    ano = df['Ano']
    nome_curso = df['Área de Avaliação'].drop_duplicates().reset_index(drop=True)
    enade = df[['Nº de Concluintes Inscritos','Nº  de Concluintes Participantes', 'Nota Bruta - FG', 'Nota Padronizada - FG', 'Nota Bruta - CE', 'Nota Padronizada - CE', 'Conceito Enade (Contínuo)', 'Conceito Enade (Faixa)']]
    ies = df[['Nome da IES', 'Sigla da IES', 'Categoria Administrativa', 'Código da IES']].drop_duplicates().reset_index(drop=True)
    municipio = df['Município do Curso'].drop_duplicates().reset_index(drop=True)
    uf = df['Sigla da UF'].drop_duplicates().reset_index(drop=True)
    
    
    

    
    
# Modify your main function to include Excel import
if __name__ == "__main__":
    engine = create_database()
    print("Database tables created successfully!")

