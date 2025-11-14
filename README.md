# Dados Públicos CNPJ
- VERSÂO ATUALIZADA PARA 2025. O código foi refeito e otimizado utilizando como referência o original, [aqui](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ)

- Fonte oficial da Receita Federal do Brasil, [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).

Essa base atualiza mensalmente, é possível ver que há varios snapshots, por tanto se atente a entrar na versão mais atual e mudar o URL (dados_rf) no código.

Nesse repositório consta um processo de ETL para **i)** baixar os arquivos; **ii)** descompactar; **iii)** ler, tratar e **iv)** inserir no banco de dados PostgreSQL.

---------------------

### Infraestrutura necessária:
- [Python] - Testado até 3.11.9
- [PostgreSQL] - Testado até 18

---------------------

### How to use:
1. Com o Postgres instalado, inicie a instância do servidor (pode ser local) e crie o banco de dados conforme o arquivo `banco_de_dados.sql`.

2. Crie um arquivo `.env` no diretório `code`, conforme as variáveis de ambiente do seu ambiente de trabalho (localhost). Utilize como referência o arquivo `.env_template`. 

   - `OUTPUT_FILES_PATH`: diretório de destino para o download dos arquivos
   - `EXTRACTED_FILES_PATH`: diretório de destino para a extração dos arquivos .zip
   - `DB_USER`: usuário do banco de dados criado pelo arquivo `banco_de_dados.sql`
   - `DB_PASSWORD`: senha do usuário do BD
   - `DB_HOST`: host da conexão com o BD
   - `DB_PORT`: porta da conexão com o BD
   - `DB_NAME`: nome da base de dados na instância (`Dados_RFB` - conforme arquivo `banco_de_dados.sql`)

3. Instale as bibliotecas necessárias, disponíveis em `requirements.txt`:
```
pip install -r requirements.txt
```

4. Execute o arquivo `ETL_coletar_dados_e_gravar_BD.py` e aguarde a finalização do processo.
   - Os arquivos são grandes. Dependendo da infraestrutura isso deve levar muitas horas para conclusão.

---------------------

### Tabelas geradas:
  - `empresa`: dados cadastrais da empresa em nível de matriz
  - `estabelecimento`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
  - `socios`: dados cadastrais dos sócios das empresas
  - `simples`: dados de MEI e Simples Nacional
  - `cnae`: código e descrição dos CNAEs
  - `quals`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.
  - `natju`: tabela de naturezas jurídicas - código e descrição.
  - `moti`: tabela de motivos da situação cadastral - código e descrição.
  - `pais`: tabela de países - código e descrição.
  - `munic`: tabela de municípios - código e descrição.


- Pelo volume de dados, as tabelas  `empresa`, `estabelecimento`, `socios` e `simples` possuem índices para a coluna `cnpj_basico`, que é a principal chave de ligação entre elas.

### Modelo de Entidade Relacionamento:
![alt text](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/blob/master/Dados_RFB_ERD.png)