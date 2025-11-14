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

# verifica se precisa baixar 
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

# cria diretório se não existir
def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

# insere em pedaços para evitar grandes transações
def to_sql(dataframe, **kwargs):
    size = 100000
    total = len(dataframe)
    name = kwargs.get("name", "table")
    for i in range(0, total, size):
        chunk = dataframe[i : i + size]
        chunk.to_sql(**kwargs)
        percent = (min(i, total) * 100) / total if total > 0 else 100
        sys.stdout.write(f"\r{name} {percent:.2f}% {i}/{total}")
        sys.stdout.flush()
    sys.stdout.write("\n")

def getEnv(env):
    return os.getenv(env)

# -------------------------------------------------------------------
# configuração .env
# -------------------------------------------------------------------
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, ".env")
if not os.path.isfile(dotenv_path):
    print('Especifique o local do arquivo .env:')
    local_env = input().strip()
    dotenv_path = os.path.join(local_env, ".env")
print("Usando .env em:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

dados_rf = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-11/"

# -------------------------------------------------------------------
# diretórios
# -------------------------------------------------------------------
try:
    output_files = getEnv("OUTPUT_FILES_PATH")
    extracted_files = getEnv("EXTRACTED_FILES_PATH")
    makedirs(output_files)
    makedirs(extracted_files)
    print(f"Diretórios: output={output_files} extract={extracted_files}")
except Exception as e:
    print("Erro na definição dos diretórios:", e)
    raise

# -------------------------------------------------------------------
# listar arquivos .zip 
# -------------------------------------------------------------------
print("Obtendo lista de arquivos...")

try:
    resp = requests.get(dados_rf, timeout=20)
    resp.raise_for_status()
    html = resp.text
except Exception as e:
    print("Erro ao acessar a URL:", e)
    sys.exit(1)

# buscar todos os nomes que terminam em .zip 
matches = re.findall(r'([A-Za-z0-9_\-./\\]+\.zip)', html, flags=re.IGNORECASE)
Files = sorted({os.path.basename(m) for m in matches})  

if not Files:
    print("Nenhum .zip encontrado na página. HTML sample (primeiros 400 chars):")
    print(html[:400])
    sys.exit(1)

print("Arquivos detectados:")
for i, f in enumerate(Files, start=1):
    print(f"{i} - {f}")

# -------------------------------------------------------------------
# download
# -------------------------------------------------------------------
def bar_progress(current, total, width=80):
    msg = f"Downloading: {current/total*100:.1f}% [{current}/{total}]"
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()

for f in Files:
    file_url = dados_rf + f
    file_path = os.path.join(output_files, f)
    print(f"\nBaixando {f} ...")
    if check_diff(file_url, file_path):
        try:
            wget.download(file_url, out=output_files, bar=bar_progress)
            print()
        except Exception as e:
            print(f"Erro ao baixar {f}: {e}")
    else:
        print("Arquivo existe e tem mesmo tamanho. Pulando.")

# -------------------------------------------------------------------
# extrair zips
# -------------------------------------------------------------------
print("\nExtraindo ZIPs...")
for f in Files:
    full = os.path.join(output_files, f)
    if not os.path.isfile(full):
        print(f"Atenção: zip não encontrado (pulando): {full}")
        continue
    try:
        print(f"Extraindo {f} ...")
        with zipfile.ZipFile(full, "r") as z:
            z.extractall(extracted_files)
    except Exception as e:
        print(f"Erro ao extrair {f}: {e}")

# -------------------------------------------------------------------
# identificar arquivos extraídos
# -------------------------------------------------------------------
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
    else:
        pass

# -------------------------------------------------------------------
# conectar ao banco
# -------------------------------------------------------------------
user = getEnv("DB_USER")
passw = getEnv("DB_PASSWORD")
host = getEnv("DB_HOST")
port = getEnv("DB_PORT")
database = getEnv("DB_NAME")

engine = create_engine(f"postgresql://{user}:{passw}@{host}:{port}/{database}")
conn = psycopg2.connect(f"dbname={database} user={user} host={host} port={port} password={passw}")
cur = conn.cursor()

# -------------------------------------------------------------------
# EMPRESA
# -------------------------------------------------------------------
print("\n=== EMPRESA ===")
with engine.begin() as conn_engine:
    conn_engine.execute(text("DROP TABLE IF EXISTS empresa;"))

for arq in arquivos_empresa:
    path = os.path.join(extracted_files, arq)
    print("Lendo", path)
    df = pd.read_csv(path, sep=";", header=None,
                     dtype={0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: object},
                     encoding="latin-1")
    df.columns = ["cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
                  "capital_social", "porte_empresa", "ente_federativo_responsavel"]
    # tratar capital 
    df["capital_social"] = df["capital_social"].astype(str).str.replace(",", ".").replace("nan", "")
    df.loc[df["capital_social"] == "", "capital_social"] = "0"
    df["capital_social"] = df["capital_social"].astype(float)
    to_sql(df, name="empresa", con=engine, if_exists="append", index=False)

# -------------------------------------------------------------------
# ESTABELECIMENTO 
# -------------------------------------------------------------------
print("\n=== ESTABELECIMENTO ===")
cur.execute("DROP TABLE IF EXISTS estabelecimento;")
conn.commit()

for arq in arquivos_estabelecimento:
    path = os.path.join(extracted_files, arq)
    print("Processando", path)
    estabelecimento_dtypes = {
        0: object, 1: object, 2: object, 3: "Int32", 4: object, 5: "Int32", 6: "Int32",
        7: "Int32", 8: object, 9: object, 10: "Int32", 11: "Int32", 12: object, 13: object,
        14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
        20: "Int32", 21: object, 22: object, 23: object, 24: object, 25: object,
        26: object, 27: object, 28: object, 29: "Int32"
    }
    try:
        for chunk in pd.read_csv(path, sep=";", header=None, dtype=estabelecimento_dtypes,
                                 encoding="latin-1", chunksize=500_000):
            chunk.columns = [
                "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial","nome_fantasia",
                "situacao_cadastral","data_situacao_cadastral","motivo_situacao_cadastral",
                "nome_cidade_exterior","pais","data_inicio_atividade","cnae_fiscal_principal",
                "cnae_fiscal_secundaria","tipo_logradouro","logradouro","numero","complemento",
                "bairro","cep","uf","municipio","ddd_1","telefone_1","ddd_2","telefone_2",
                "ddd_fax","fax","correio_eletronico","situacao_especial","data_situacao_especial"
            ]
            to_sql(chunk, name="estabelecimento", con=engine, if_exists="append", index=False)
    except Exception as e:
        print(f"Erro lendo estabelecimento {path}: {e}")

# -------------------------------------------------------------------
# SOCIOS
# -------------------------------------------------------------------
print("\n=== SOCIOS ===")
cur.execute("DROP TABLE IF EXISTS socios;")
conn.commit()

for arq in arquivos_socios:
    path = os.path.join(extracted_files, arq)
    print("Lendo", path)
    df = pd.read_csv(path, sep=";", header=None,
                     dtype={0: object, 1: "Int32", 2: object, 3: object, 4: "Int32",
                            5: "Int32", 6: "Int32", 7: object, 8: object, 9: "Int32", 10: "Int32"},
                     encoding="latin-1")
    df.columns = ["cnpj_basico","identificador_socio","nome_socio_razao_social","cpf_cnpj_socio",
                  "qualificacao_socio","data_entrada_sociedade","pais","representante_legal",
                  "nome_do_representante","qualificacao_representante_legal","faixa_etaria"]
    to_sql(df, name="socios", con=engine, if_exists="append", index=False)

# -------------------------------------------------------------------
# SIMPLES 
# -------------------------------------------------------------------
print("\n=== SIMPLES ===")
cur.execute("DROP TABLE IF EXISTS simples;")
conn.commit()

for arq in arquivos_simples:
    path = os.path.join(extracted_files, arq)
    print("Lendo", path)
    try:
        total = sum(1 for _ in open(path, "r", encoding="latin-1"))
        partes = total // 1_000_000 + 1
        skip = 0
        for i in range(partes):
            df = pd.read_csv(path, sep=";", header=None, skiprows=skip, nrows=1_000_000,
                             dtype={0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: "Int32"},
                             encoding="latin-1")
            df.columns = ["cnpj_basico","opcao_pelo_simples","data_opcao_simples","data_exclusao_simples",
                          "opcao_mei","data_opcao_mei","data_exclusao_mei"]
            to_sql(df, name="simples", con=engine, if_exists="append", index=False)
            skip += 1_000_000
    except Exception as e:
        print(f"Erro processando simples {path}: {e}")

# -------------------------------------------------------------------
# tabelas pequenas: cnae, moti, munic, natju, pais, quals
# -------------------------------------------------------------------
def load_small_table(files, columns, table_name, dtype):
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()
    for arq in files:
        path = os.path.join(extracted_files, arq)
        print("Lendo", path)
        df = pd.read_csv(path, sep=";", header=None, dtype=dtype, encoding="latin-1")
        df.columns = columns
        to_sql(df, name=table_name, con=engine, if_exists="append", index=False)

print("\n=== CNAE ===")
load_small_table(arquivos_cnae, ["codigo", "descricao"], "cnae", {0: object, 1: object})

print("\n=== MOTI ===")
load_small_table(arquivos_moti, ["codigo", "descricao"], "moti", {0: "Int32", 1: object})

print("\n=== MUNIC ===")
load_small_table(arquivos_munic, ["codigo", "descricao"], "munic", {0: "Int32", 1: object})

print("\n=== NATJU ===")
load_small_table(arquivos_natju, ["codigo", "descricao"], "natju", {0: "Int32", 1: object})

print("\n=== PAIS ===")
load_small_table(arquivos_pais, ["codigo", "descricao"], "pais", {0: "Int32", 1: object})

print("\n=== QUALS ===")
load_small_table(arquivos_quals, ["codigo", "descricao"], "quals", {0: "Int32", 1: object})

# -------------------------------------------------------------------
# criar índices
# -------------------------------------------------------------------
print("\nCriando índices...")
cur.execute("CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);")
cur.execute("CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);")
conn.commit()

print("\nProcesso concluído com sucesso!")
