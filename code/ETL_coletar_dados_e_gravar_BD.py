import pathlib
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import pandas as pd
import psycopg2
import re
import sys
import requests
import wget
import zipfile

# ===============================
# FUNÇÃO DE PROGRESSO UNIVERSAL
# ===============================
def show_progress(label, processed, total):
    if total == 0:
        percent = 100
    else:
        percent = (processed / total) * 100

    sys.stdout.write(
        f"\r{label:<18} {percent:6.2f}%  {processed:,}/{total:,}"
    )
    sys.stdout.flush()

# ===============================
# FUNÇÃO UNIVERSAL PARA LOAD
# ===============================
def load_table_with_progress(path, table_name, columns, dtype, engine, chunksize=None):
    print(f"\n=== {table_name.upper()} ===")

    # Conta total de linhas
    print("Contando linhas...")
    total_linhas = sum(1 for _ in open(path, "r", encoding="latin-1"))
    print(f"Total de linhas: {total_linhas:,}")

    processed = 0

    # Para tabelas grandes
    if chunksize:
        for chunk in pd.read_csv(
            path, sep=";", header=None, dtype=dtype,
            encoding="latin-1", chunksize=chunksize
        ):

            chunk.columns = columns
            processed += len(chunk)
            show_progress(table_name, processed, total_linhas)

            chunk.to_sql(table_name, engine, if_exists="append", index=False)

    # Para tabelas pequenas
    else:
        df = pd.read_csv(
            path, sep=";", header=None, dtype=dtype, encoding="latin-1"
        )
        df.columns = columns
        df.to_sql(table_name, engine, if_exists="append", index=False)

        processed = total_linhas
        show_progress(table_name, processed, total_linhas)

    sys.stdout.write("\n")
    print(f"Finalizado: {table_name}")

# ============================================
# VERIFICA DIFERENÇA
# ============================================
def check_diff(url, file_name):
    if not os.path.isfile(file_name):
        return True
    try:
        response = requests.head(url, timeout=20)
    except Exception:
        return True
    new_size = int(response.headers.get("content-length", 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True
    return False

def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def getEnv(env):
    return os.getenv(env)

# ===============================
# CONFIG .ENV
# ===============================
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, ".env")
if not os.path.isfile(dotenv_path):
    print('Especifique o local do arquivo .env:')
    dotenv_path = os.path.join(input().strip(), ".env")
print("Usando .env em:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

dados_rf = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-11/"

# ===============================
# DIRETÓRIOS
# ===============================
output_files = getEnv("OUTPUT_FILES_PATH")
extracted_files = getEnv("EXTRACTED_FILES_PATH")
makedirs(output_files); makedirs(extracted_files)
print(f"Diretórios: output={output_files} extract={extracted_files}")

# ===============================
# LISTAR ARQUIVOS
# ===============================
print("Obtendo lista de arquivos...")

resp = requests.get(dados_rf, timeout=20)
resp.raise_for_status()
html = resp.text

matches = re.findall(r'([A-Za-z0-9_\-./\\]+\.zip)', html, flags=re.IGNORECASE)
Files = sorted({os.path.basename(m) for m in matches})

if not Files:
    print("Nenhum .zip encontrado.")
    sys.exit(1)

print("Arquivos detectados:")
for f in Files:
    print(" -", f)

# ===============================
# DOWNLOAD
# ===============================
def bar_progress(current, total, width=80):
    msg = f"Downloading: {current/total*100:.1f}% [{current}/{total}]"
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()

for f in Files:
    url = dados_rf + f
    path = os.path.join(output_files, f)
    print(f"\nBaixando {f} ...")
    if check_diff(url, path):
        wget.download(url, out=output_files, bar=bar_progress)
        print()
    else:
        print("Arquivo existe. Pulando.")

# ===============================
# EXTRAIR
# ===============================
print("\nExtraindo ZIPs...")
for f in Files:
    full = os.path.join(output_files, f)
    try:
        print(f"Extraindo {f} ...")
        with zipfile.ZipFile(full, "r") as z:
            z.extractall(extracted_files)
    except Exception as e:
        print("Erro ao extrair:", e)

# ===============================
# IDENTIFICAR ARQUIVOS INTERNOS
# ===============================
Items = os.listdir(extracted_files)

arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []

for item in Items:
    name = item.upper()

    if "EMPRECSV" in name:
        arquivos_empresa.append(item)
    elif "ESTABELE" in name:
        arquivos_estabelecimento.append(item)
    elif "SOCIOCSV" in name:
        arquivos_socios.append(item)
    elif "SIMP" in name and "CSV" in name:
        arquivos_simples.append(item)
    elif "CNAECSV" in name:
        arquivos_cnae.append(item)
    elif "MOTICSV" in name:
        arquivos_moti.append(item)
    elif "MUNICCSV" in name:
        arquivos_munic.append(item)
    elif "NATJUCSV" in name:
        arquivos_natju.append(item)
    elif "PAISCSV" in name:
        arquivos_pais.append(item)
    elif "QUALSCSV" in name:
        arquivos_quals.append(item)

# ===============================
# BANCO
# ===============================
user = getEnv("DB_USER")
passw = getEnv("DB_PASSWORD")
host = getEnv("DB_HOST")
port = getEnv("DB_PORT")
database = getEnv("DB_NAME")

engine = create_engine(f"postgresql://{user}:{passw}@{host}:{port}/{database}")
conn = psycopg2.connect(f"dbname={database} user={user} host={host} port={port} password={passw}")
cur = conn.cursor()

# ===============================
# EMPRESA
# ===============================
cur.execute("DROP TABLE IF EXISTS empresa;")
conn.commit()

for arq in arquivos_empresa:
    load_table_with_progress(
        path=os.path.join(extracted_files, arq),
        table_name="empresa",
        columns=[
            "cnpj_basico","razao_social","natureza_juridica",
            "qualificacao_responsavel","capital_social",
            "porte_empresa","ente_federativo_responsavel"
        ],
        dtype={0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: object},
        engine=engine,
        chunksize=500_000
    )

# ===============================
# ESTABELECIMENTO
# ===============================
cur.execute("DROP TABLE IF EXISTS estabelecimento;")
conn.commit()

for arq in arquivos_estabelecimento:
    load_table_with_progress(
        path=os.path.join(extracted_files, arq),
        table_name="estabelecimento",
        columns=[
            "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
            "nome_fantasia","situacao_cadastral","data_situacao_cadastral",
            "motivo_situacao_cadastral","nome_cidade_exterior","pais",
            "data_inicio_atividade","cnae_fiscal_principal","cnae_fiscal_secundaria",
            "tipo_logradouro","logradouro","numero","complemento","bairro","cep",
            "uf","municipio","ddd_1","telefone_1","ddd_2","telefone_2","ddd_fax",
            "fax","correio_eletronico","situacao_especial","data_situacao_especial"
        ],
        dtype={
            0: object, 1: object, 2: object, 3: "Int32", 4: object, 5: "Int32", 6: "Int32",
            7: "Int32", 8: object, 9: object, 10: "Int32", 11: "Int32", 12: object, 13: object,
            14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
            20: "Int32", 21: object, 22: object, 23: object, 24: object, 25: object,
            26: object, 27: object, 28: object, 29: "Int32"
        },
        engine=engine,
        chunksize=500_000
    )

# ===============================
# SOCIOS
# ===============================
cur.execute("DROP TABLE IF EXISTS socios;")
conn.commit()

for arq in arquivos_socios:
    load_table_with_progress(
        path=os.path.join(extracted_files, arq),
        table_name="socios",
        columns=[
            "cnpj_basico","identificador_socio","nome_socio_razao_social",
            "cpf_cnpj_socio","qualificacao_socio","data_entrada_sociedade",
            "pais","representante_legal","nome_do_representante",
            "qualificacao_representante_legal","faixa_etaria"
        ],
        dtype={
            0: object, 1: "Int32", 2: object, 3: object, 4: "Int32",
            5: "Int32", 6: "Int32", 7: object, 8: object, 9: "Int32", 10: "Int32"
        },
        engine=engine,
        chunksize=500_000
    )

# ===============================
# SIMPLES
# ===============================
cur.execute("DROP TABLE IF EXISTS simples;")
conn.commit()

for arq in arquivos_simples:
    load_table_with_progress(
        path=os.path.join(extracted_files, arq),
        table_name="simples",
        columns=[
            "cnpj_basico","opcao_pelo_simples","data_opcao_simples",
            "data_exclusao_simples","opcao_mei","data_opcao_mei","data_exclusao_mei"
        ],
        dtype={
            0: object, 1: object, 2: "Int32", 3: "Int32",
            4: object, 5: "Int32", 6: "Int32"
        },
        engine=engine,
        chunksize=500_000
    )

# ===============================
# TABELAS PEQUENAS
# ===============================
small_tables = [
    (arquivos_cnae, "cnae", ["codigo","descricao"], {0: object, 1: object}),
    (arquivos_moti, "moti", ["codigo","descricao"], {0: "Int32", 1: object}),
    (arquivos_munic, "munic", ["codigo","descricao"], {0: "Int32", 1: object}),
    (arquivos_natju, "natju", ["codigo","descricao"], {0: "Int32", 1: object}),
    (arquivos_pais, "pais", ["codigo","descricao"], {0: "Int32", 1: object}),
    (arquivos_quals, "quals", ["codigo","descricao"], {0: "Int32", 1: object}),
]

for files, name, cols, types in small_tables:
    cur.execute(f"DROP TABLE IF EXISTS {name};")
    conn.commit()
    for arq in files:
        load_table_with_progress(
            path=os.path.join(extracted_files, arq),
            table_name=name,
            columns=cols,
            dtype=types,
            engine=engine,
            chunksize=None
        )

# ===============================
# ÍNDICES
# ===============================
print("\nCriando índices...")
cur.execute("CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);")
conn.commit()

print("\nProcesso concluído com sucesso!")
