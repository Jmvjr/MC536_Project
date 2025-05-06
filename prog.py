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
    codigo_ies = Column(Integer, nullable=False)
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


#function to import CSV data into the database
def import_csv_enade(engine):
    """
    Importa dados dos arquivos CSV do ENADE e insere nas tabelas do PostgreSQL
    """
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
        #tratando tabelas
        mask = df['Conceito Enade (Faixa)'] == 'SC'
        df = df[~mask]
        mask = df['Sigla da UF'].isna()
        df = df[~mask]
        mask = df[['Nota Bruta - FG', 'Nota Padronizada - FG', 'Nota Bruta - CE', 'Nota Padronizada - CE', 'Conceito Enade (Contínuo)', 'Conceito Enade (Faixa)', 'Sigla da IES']].isna().any(axis=1)
        df = df[~mask]
        df = df.reset_index(drop=True)
        
        # Dicionários para rastrear IDs
        uf_ids = {}
        municipio_ids = {}
        ies_ids = {}
        curso_ids = {}
        enade_ids = {}
        
        # 1. Inserir UFs
        print("Inserindo UFs...")
        for sigla_uf in df['Sigla da UF'].unique():
            # Verificar se UF já existe
            uf_obj = UF(
                sigla_uf=sigla_uf,
                nome=f"Estado de {sigla_uf}",
                qtd_habitantes=0  # Valor padrão
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
            
            municipio_obj = Municipio(
                id_uf=uf_ids[sigla_uf],
                nome_municipio=nome_municipio,
                qtd_IES=0,
                qtd_ESCOLAS=0,
                media_ideb=0.0,
                media_saeb=0.0,
                media_enade=0.0,
                total_participantes=0
            )
            session.add(municipio_obj)
            session.flush()
            # Usar um composto de cidade+UF como chave já que pode haver cidades homônimas
            municipio_ids[f"{nome_municipio}_{sigla_uf}"] = municipio_obj.id_municipio
        
        # 3. Inserir IES
        print("Inserindo IES...")

        # Extrair as colunas relevantes
        ies_completo = df[['Código da IES', 'Nome da IES', 'Sigla da IES', 'Categoria Administrativa', 
                          'Município do Curso', 'Sigla da UF']]

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
                ies_obj = IES(
                    id_municipio=municipio_ids[municipio_key],
                    nome_ies=nome_ies,
                    sigla_ies=sigla_ies,
                    codigo_ies=cod_ies,
                    rede=categoria
                )
                session.add(ies_obj)
                session.flush()
                # Usar uma chave composta para rastrear IES por código e município
                ies_ids[(cod_ies, municipio_key)] = ies_obj.id_ies
        
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
                curso_obj = Curso(
                    id_IES=ies_ids[(cod_ies, municipio_key)],
                    nome_curso=nome_curso
                )
                session.add(curso_obj)
                session.flush()
                curso_ids[cod_curso] = curso_obj.id_curso
        
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
                    enade_ids[(cod_curso, ano_enade)] = enade_obj.id_enade
                    
                    # 6. Inserir Ano relacionando curso e ENADE
                    ano_obj = Ano(
                        id_curso=curso_ids[cod_curso],
                        id_enade=enade_obj.id_enade,
                        ano=ano_enade
                    )
                    session.add(ano_obj)
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
        import traceback
        traceback.print_exc()
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
        print("Processando UFs...")
        for sigla_uf in df['Sigla da UF'].dropna().unique():
            # Verificar se é NaN
            if pd.isna(sigla_uf) or sigla_uf == 'nan':
                continue
            
            # Garantir que sigla_uf é uma string
            sigla_uf = str(sigla_uf).strip()
            
            # Verificar se UF já existe na sessão
            uf_obj = session.query(UF).filter_by(sigla_uf=sigla_uf).first()
            if not uf_obj:
                uf_obj = UF(
                    sigla_uf=sigla_uf,
                    nome=f"Estado de {sigla_uf}",
                    qtd_habitantes=0
                )
                session.add(uf_obj)
                session.flush()
            uf_ids[sigla_uf] = uf_obj.id
        
        # 2. Inserir/Obter Municípios
        print("Processando Municípios...")
        for _, row in df[['Sigla da UF', 'Código do Município', 'Nome do Município']].drop_duplicates().iterrows():
            sigla_uf = row['Sigla da UF']
            nome_municipio = row['Nome do Município']
            cod_municipio = str(row['Código do Município'])
            
            # Chave composta para identificar o município
            municipio_key = f"{nome_municipio}_{sigla_uf}"
            
            # Verificar se já existe
            municipio_obj = session.query(Municipio).filter_by(
                nome_municipio=nome_municipio,
                id_uf=uf_ids[sigla_uf]
            ).first()
            
            if not municipio_obj:
                municipio_obj = Municipio(
                    id_uf=uf_ids[sigla_uf],
                    nome_municipio=nome_municipio,
                    qtd_IES=0,
                    qtd_ESCOLAS=0,
                    media_ideb=0.0,
                    media_saeb=0.0,
                    media_enade=0.0,
                    total_participantes=0
                )
                session.add(municipio_obj)
                session.flush()
            
            municipio_ids[municipio_key] = municipio_obj.id_municipio
        
        # 3. Inserir Escolas
        print("Processando Escolas...")
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
                escola_obj = session.query(Escola).filter_by(cod_escola=codigo_escola).first()
                if not escola_obj:
                    escola_obj = Escola(
                        id_municipio=municipio_ids[municipio_key],
                        nome_escola=nome_escola,
                        cod_escola=codigo_escola,
                        rede=rede
                    )
                    session.add(escola_obj)
                    session.flush()
                
                escola_ids[codigo_escola] = escola_obj.id_escola
        
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
                    ind_rend = convert_to_float_or_none(row[f'Indicador_Rendimento_{ano}'])
                    nota_ideb = convert_to_float_or_none(row[f'Nota_ideb_{ano}'])
                    
                    # SAEB - usando convert_to_float_or_none para tratar valores "-" 
                    nota_mat = convert_to_float_or_none(row[f'Nota_SAEB_{ano}_Mat'])
                    nota_port = convert_to_float_or_none(row[f'Nota_SAEB_{ano}_Port'])
                    nota_padrao = convert_to_float_or_none(row[f'Nota_padronizada_SAEB_{ano}'])
                    
                    # Criar objetos apenas se temos pelo menos alguns dados
                    if any([nota_ideb is not None, ind_rend is not None, nota_padrao is not None]):
                        # 4.1 Inserir IDEB
                        ideb_obj = IDEB(
                            rend_1=rend_1,
                            rend_2=rend_2,
                            rend_3=rend_3,
                            rend_4=rend_4,
                            ind_rend=ind_rend,
                            nota_ideb=nota_ideb
                        )
                        session.add(ideb_obj)
                        session.flush()
                        
                        # 4.2 Inserir SAEB
                        saeb_obj = SAEB(
                            nota_mat=nota_mat,
                            nota_port=nota_port,
                            nota_padrao=nota_padrao
                        )
                        session.add(saeb_obj)
                        session.flush()
                        
                        # 4.3 Inserir Ano relacionando a escola com IDEB e SAEB
                        # Verificar se já existe um registro Ano para este ano
                        ano_obj = Ano(
                            id_ideb=ideb_obj.id_ideb,
                            id_saeb=saeb_obj.id_saeb,
                            ano=ano
                        )
                        session.add(ano_obj)
                        session.flush()
                        
                        # Guardar os IDs para referência futura
                        ideb_ids[(codigo_escola, ano)] = ideb_obj.id_ideb
                        saeb_ids[(codigo_escola, ano)] = saeb_obj.id_saeb
                    
                except Exception as e:
                    print(f"Erro ao processar dados do ano {ano} para escola {codigo_escola}: {e}")
                    continue
        
        # Atualizar estatísticas dos municípios
        print("Atualizando estatísticas dos municípios...")
        for municipio_key, municipio_id in municipio_ids.items():
            # Contagem de escolas
            qtd_escolas = session.query(Escola).filter_by(id_municipio=municipio_id).count()
            
            # Obter todas as escolas deste município
            escolas_do_municipio = session.query(Escola.cod_escola).filter_by(id_municipio=municipio_id).all()
            codigos_escolas = [e[0] for e in escolas_do_municipio]
            
            ideb_valores = []
            saeb_valores = []
            
            # Para cada escola, buscar valores nos dicionários de IDs
            for codigo_escola in codigos_escolas:
                for ano in anos:
                    # Verificar IDEB
                    if (codigo_escola, ano) in ideb_ids:
                        id_ideb = ideb_ids[(codigo_escola, ano)]
                        ideb_obj = session.query(IDEB).filter_by(id_ideb=id_ideb).first()
                        if ideb_obj and ideb_obj.nota_ideb is not None:
                            ideb_valores.append(ideb_obj.nota_ideb)
                            
                    # Verificar SAEB
                    if (codigo_escola, ano) in saeb_ids:
                        id_saeb = saeb_ids[(codigo_escola, ano)]
                        saeb_obj = session.query(SAEB).filter_by(id_saeb=id_saeb).first()
                        if saeb_obj and saeb_obj.nota_padrao is not None:
                            saeb_valores.append(saeb_obj.nota_padrao)
            
            # Calcular médias
            media_ideb = sum(ideb_valores) / len(ideb_valores) if ideb_valores else 0.0
            media_saeb = sum(saeb_valores) / len(saeb_valores) if saeb_valores else 0.0
            
            # Atualizar município
            municipio = session.query(Municipio).filter_by(id_municipio=municipio_id).first()
            if municipio:
                municipio.qtd_ESCOLAS = qtd_escolas
                municipio.media_ideb = media_ideb
                municipio.media_saeb = media_saeb
        
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
        import traceback
        traceback.print_exc()
    finally:
        # Fechar a sessão
        session.close()
    
    
# Modify your main function to include Excel import
if __name__ == "__main__":
    engine = create_database()
    print("Database tables created successfully!")
    import_csv_enade(engine)
    import_csv_ideb(engine)

