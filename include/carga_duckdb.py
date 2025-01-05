import duckdb
from dotenv import load_dotenv
from pathlib import Path
import os

def load_settings():
    dotenv_path = Path.cwd() / '.env'
    load_dotenv(dotenv_path=dotenv_path)

    settings = {
        "db_host_prod": os.getenv("DB_HOST_PROD"),
        "db_db_homolog": os.getenv("DB_NAME_HOMOLOG"),
        "db_user_prod": os.getenv("DB_USER_PROD"),
        "db_pass_prod": os.getenv("DB_PASS_PROD"),
        "db_port_prod": os.getenv("DB_PORT_PROD"),
    }
    return settings

def connect_postgres():
    settings = load_settings()
    conn = duckdb.connect()

    conn.execute("INSTALL postgres_scanner;")
    conn.execute("LOAD postgres_scanner;")


    postgres_connection = f"postgres://{settings['db_user_prod']}:{settings['db_pass_prod']}@{settings['db_host_prod']}:{settings['db_port_prod']}/{settings['db_db_homolog']}"

    conn.execute(f"ATTACH '{postgres_connection}' AS postgres_db (TYPE POSTGRES);")

    return conn

def create_table_dl():
    conn_pg = connect_postgres()


    create_table_query = """
    CREATE OR REPLACE TABLE postgres_db.az_bucket (
        id INTEGER,
        id_usuario VARCHAR,
        nome_usuario VARCHAR,
        sistema_origem VARCHAR,
        navegador VARCHAR,
        acao_realizada VARCHAR,
        created_at TIMESTAMP
    );
    """
    conn_pg.execute(create_table_query)
    conn_pg.close()

def insert_table_dl(id: int, id_usuarios: str, nome_usuario: str, sistema_origem: str, navegador: str, acao: str):
    conn_pg = connect_postgres()

    insert_query = """
    INSERT INTO postgres_db.az_bucket
    (id, id_usuario, nome_usuario, sistema_origem, navegador, acao_realizada, created_at)
    VALUES
    (?, ?, ?, ?, ?, ?, NOW());
    """
    data = (id, id_usuarios, nome_usuario, sistema_origem, navegador, acao)

    conn_pg.execute(insert_query,data)


    result = conn_pg.execute("SELECT * FROM postgres_db.az_bucket").fetchall()


    print("Dados inseridos na tabela:")
    for row in result:
        print(row)

                                        
    conn_pg.close()

criar_tabela = create_table_dl()
insert_table_dl(1, 'user2233', 'Maria Fernanda', 'Sistema de compras', 'Edge', 'Login')
insert_table_dl(2, 'user1122', 'João Pedro', 'Sistema Financeiro', 'Chrome', 'Login')
insert_table_dl(3, 'user3344', 'Ana Luiza', 'Sistema de Estoque', 'Firefox', 'Consulta')
insert_table_dl(4, 'user5566', 'Carlos Henrique', 'Gestão de RH', 'Safari', 'Atualização')
insert_table_dl(5, 'user7788', 'Beatriz Souza', 'Sistema de Compras', 'Edge', 'Login')
insert_table_dl(6, 'user9900', 'Gabriel Lima', 'Sistema de Vendas', 'Chrome', 'Cadastro')
insert_table_dl(7, 'user1245', 'Luciana Mendes', 'Sistema de Logística', 'Firefox', 'Login')
insert_table_dl(8, 'user2456', 'Roberto Carlos', 'Sistema de Compras', 'Chrome', 'Consulta')
insert_table_dl(9, 'user4578', 'Carla Silva', 'Sistema de Suporte', 'Safari', 'Atualização')
insert_table_dl(10, 'user6789', 'Paulo Cesar', 'Sistema Financeiro', 'Safari', 'Atualização')
insert_table_dl(11, 'user8923', 'Vanessa Oliveira', 'Sistema de Estoque', 'Firefox', 'Consulta')
insert_table_dl(12, 'user3345', 'Felipe Araújo', 'Sistema de Compras', 'Edge', 'Login')
insert_table_dl(13, 'user5567', 'Marcela Pereira', 'Sistema Financeiro', 'Chrome', 'Cadastro')
insert_table_dl(14, 'user7790', 'Luiz Gustavo', 'Gestão de RH', 'Safari', 'Atualização')
insert_table_dl(15, 'user9912', 'Patrícia Andrade', 'Gestão de RH', 'Safari', 'Atualização')
insert_table_dl(16, 'user1234', 'Ricardo Alves', 'Sistema de Suporte', 'Edge', 'Login')
insert_table_dl(17, 'user2457', 'Aline Castro', 'Sistema de Logística', 'Chrome', 'Consulta')
insert_table_dl(18, 'user4579', 'Eduardo Santos', 'Sistema de Compras', 'Firefox', 'Login')
insert_table_dl(19, 'user6790', 'Isabela Ramos', 'Sistema de Vendas', 'Safari', 'Cadastro')
insert_table_dl(20, 'user8924', 'Renato Lima', 'Sistema de Estoque', 'Edge', 'Atualização')
insert_table_dl(21,'user9900','Jose Almeida','Sistema de Estoque','Firefox','Login')


