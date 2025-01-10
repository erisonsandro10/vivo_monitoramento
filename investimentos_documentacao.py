# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoramento do Processo de Investimentos

# COMMAND ----------

# MAGIC %run /Lima/stage/libs/notebook/initialization 

# COMMAND ----------

# Inicia configurando o ambiente
configure_environment()

# Importar apenas as funções que o notebook irá usar. Caso use muitas funções
# pode importar todas (usando "*")
from util.dates import subtract_days, subtract_months
from util.widgets import init_widget_start_date, init_widget_end_date
from google_analytics_4 import run_gold_bq
from pyspark.sql import functions as F
from data_ingestion import *
from big_query import *
from google_storage import *

from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DateType
import pytz
# import great_expectations as gx


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cria os Parametros

# COMMAND ----------

# Setar o início e fim da execução via JOB
START_DATE = init_widget_start_date(subtract_days(1))
END_DATE = init_widget_end_date(subtract_days(1))

LOG_TABLE = "ecp.bq_profiler.logs_investimentos"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Ajuste da Data Inicial (`get_adjusted_start_date`)
# MAGIC Essa etapa ajusta a data inicial.
# MAGIC
# MAGIC ##### INPUT
# MAGIC - **`start_date`**: A data inicial no formato `YYYY-MM-DD`.
# MAGIC - **`days`** (opcional): Quantos dias anteriores na data (padrão: 30 dias).
# MAGIC - **`reprocessamento`**: Se for reprocessamento.
# MAGIC
# MAGIC ##### OUTPUT
# MAGIC - Se for reprocessamento, devolve a mesma data inicial.
# MAGIC - Se não, devolve a data ajustada subtraindo o número de dias informado.
# MAGIC
# MAGIC #### Exemplo
# MAGIC ```python
# MAGIC get_adjusted_start_date("2025-01-01", days=15, reprocessamento=False)
# MAGIC # Resultado: "2024-12-17"
# MAGIC

# COMMAND ----------

def get_adjusted_start_date(start_date, days=30, reprocessamento=False):

    if reprocessamento:
        data = START_DATE
        return start_date
    else:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        data = (datetime.now(pytz.timezone("America/Sao_Paulo")) - timedelta(days=1)).strftime("%Y-%m-%d")
        return (start_date_obj - timedelta(days=days)).strftime("%Y-%m-%d")


# COMMAND ----------

# MAGIC %md
# MAGIC ## `logs_datas_faltantes`
# MAGIC
# MAGIC
# MAGIC A função `logs_datas_faltantes` retorna as datas ausentes em uma tabela específica e registra um log contendo
# MAGIC - informações sobre as datas faltantes 
# MAGIC - quantidade de datas ausentes 
# MAGIC - status do processamento.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Parâmetros
# MAGIC
# MAGIC | Parâmetro         | Tipo        | Descrição                                                                                       |
# MAGIC |-------------------|-------------|-------------------------------------------------------------------------------------------------|
# MAGIC | `dataset`         | `str`       | Nome do dataset onde a tabela está localizada.                                                 |
# MAGIC | `table`           | `str`       | Nome da tabela a ser consultada.                                                               |
# MAGIC | `start_date`      | `str`       | Data de início do intervalo a ser analisado (no formato `YYYY-MM-DD`).                         |
# MAGIC | `end_date`        | `str`       | Data de término do intervalo a ser analisado (no formato `YYYY-MM-DD`).                        |
# MAGIC | `name_column_date`| `str`       | Nome da coluna que contém as datas na tabela.                                                  |
# MAGIC | `reprocessamento` | `bool`      | Indica se o reprocessamento está habilitado. Se `True`, utiliza a `start_date` fornecida.      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ### Retorno
# MAGIC
# MAGIC A função retorna um DataFrame do Spark com o seguinte esquema:
# MAGIC
# MAGIC | Coluna             | Tipo               | Descrição                                                                 |
# MAGIC |--------------------|--------------------|---------------------------------------------------------------------------|
# MAGIC | `data`             | `str`              | Data em que o log foi gerado.                                             |
# MAGIC | `dataset`          | `str`              | Nome do dataset.                                                          |
# MAGIC | `table`            | `str`              | Nome da tabela analisada.                                                 |
# MAGIC | `qtd`              | `int`              | Quantidade de datas faltantes.                                            |
# MAGIC | `status_date`      | `str`              | Status do processamento: `SUCESS` se não houver datas ausentes, caso contrário `ERROR`. |
# MAGIC | `datas_faltantes`  | `list[str]`        | Lista de datas que estão faltando no intervalo especificado.              |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ### Lógica Interna
# MAGIC
# MAGIC **Ajuste da Data Inicial**:
# MAGIC    - A data inicial é ajustada usando a função `get_adjusted_start_date`.
# MAGIC    - Se o parâmetro `reprocessamento` for `True`, utiliza a `start_date`. Caso contrário, utiliza a data do dia anterior ao atual.
# MAGIC
# MAGIC **Consulta ao BigQuery**:
# MAGIC    - Realiza uma query para obter as datas disponíveis no intervalo especificado (`adjusted_start_date` até `end_date`).
# MAGIC
# MAGIC **Criação de Datas Esperadas**:
# MAGIC    - Utiliza o Spark para gerar uma sequência de todas as datas no intervalo.
# MAGIC
# MAGIC **Verifica se existe datas ausentes**:
# MAGIC    - Realiza um `LEFT ANTI JOIN` entre as datas esperadas e as datas retornadas pela consulta.
# MAGIC
# MAGIC **Criação Log**:
# MAGIC    - Cria um DataFrame com informações como datas ausentes, quantidade de datas, status e nome da tabela.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Observações
# MAGIC - A função depende do Spark e do BigQuery para a execução. Verifique a configuração do cluster e credenciais antes de executar.
# MAGIC
# MAGIC

# COMMAND ----------

def logs_datas_faltantes(dataset, table, start_date, end_date, name_column_date, reprocessamento=False):
    """Função que retorna as datas faltantes da tabela e registra um log"""

    adjusted_start_date = get_adjusted_start_date(start_date, reprocessamento=reprocessamento)
    
    if reprocessamento:
        data = start_date
        print(data)
    else:
        data = (datetime.now(pytz.timezone("America/Sao_Paulo")) - timedelta(days=1)).strftime("%Y-%m-%d")
        print(data)

    query = f"""
        SELECT DISTINCT {name_column_date}
        FROM telefonica-digitalsales.{dataset}.{table}
        WHERE {name_column_date} BETWEEN '{adjusted_start_date}' AND '{end_date}'
        ORDER BY {name_column_date} DESC
    """
    print(query)

    df_atual = bq_query_df(query)

    dates_df = spark.sql(f"""
        SELECT SEQUENCE(DATE('{adjusted_start_date}'), DATE('{end_date}'), INTERVAL 1 DAY) AS expected_dates
    """).withColumn("date", F.explode(F.col("expected_dates"))).select("date")


    # Encontrar datas faltantes usando LEFT ANTI JOIN
    datas_faltantes_df = dates_df.join(
        df_atual,
        dates_df["date"] == df_atual[name_column_date],
        "left_anti"
    )

    # Coletar as datas faltantes como uma lista de strings
    datas_faltantes = [row['date'].strftime('%Y-%m-%d') for row in datas_faltantes_df.collect()]

    # Contar o número de datas faltantes
    qtd = len(datas_faltantes)

    # Determinar o status com base nas datas faltantes
    status = "SUCESS" if qtd == 0 else "ERROR"
 
    # Criar o esquema explicitamente
    schema = StructType([
        StructField("data", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("table", StringType(), True),
        StructField("qtd", IntegerType(), True),
        StructField("status_date", StringType(), True),
        StructField("datas_faltantes", ArrayType(StringType()), True)
    ])

    # Criar o DataFrame final com a lista de datas faltantes
    log_data = [(data, dataset, table, qtd, status, datas_faltantes)]
    result_df = spark.createDataFrame(log_data, schema=schema)

    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##`qa_table_investimento`
# MAGIC
# MAGIC Realiza uma análise de qualidade no processo de investimentos. <br> Ela valida os dados diários de custo por plataforma, comparando o custo atual com o custo do dia anterior e categorizando possíveis problemas ou inconsistências.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ### Parâmetros
# MAGIC
# MAGIC | Parâmetro         | Tipo   | Descrição                                                                                  |
# MAGIC |-------------------|--------|------------------------------------------------------------------------------------------|
# MAGIC | `dataset`         | `str`  | Nome do dataset onde a tabela está localizada.                                            |
# MAGIC | `table`           | `str`  | Nome da tabela a ser analisada.                                                           |
# MAGIC | `start_date`      | `str`  | Data inicial do intervalo de análise (no formato `YYYY-MM-DD`).                           |
# MAGIC | `end_date`        | `str`  | Data final do intervalo de análise (no formato `YYYY-MM-DD`).                             |
# MAGIC | `name_column_date`| `str`  | Nome da coluna que contém as datas na tabela.                                             |
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Retorno
# MAGIC
# MAGIC A função retorna um DataFrame contendo os seguintes campos:
# MAGIC
# MAGIC | Coluna                   | Tipo     | Descrição                                                                 |
# MAGIC |--------------------------|----------|---------------------------------------------------------------------------|
# MAGIC | `data`                  | `str`    | Data da análise.                                                         |
# MAGIC | `dataset`               | `str`    | Nome do dataset analisado.                                               |
# MAGIC | `table`                 | `str`    | Nome da tabela analisada.                                                |
# MAGIC | `plataforma`            | `str`    | Nome da plataforma (ex.: Facebook, Twitter).                             |
# MAGIC | `total_custo`           | `float`  | Custo total do dia atual.                                                |
# MAGIC | `total_custo_dia_anterior`| `float`  | Custo total do dia anterior.                                             |
# MAGIC | `diferenca_percentual`  | `float`  | Diferença percentual entre o custo atual e o custo do dia anterior.       |
# MAGIC | `status`                | `str`    | Status do custo (ex.: "VALOR ZERADO", "OK", "CRESCIMENTO ANORMAL >150%"). |
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC <br> 
# MAGIC
# MAGIC ### Lógica Interna
# MAGIC
# MAGIC **Agregação de Dados Diários**:
# MAGIC    - Calcula o custo diário total por plataforma dentro do intervalo especificado.
# MAGIC    - Filtra por `facebook`, `twitter`, `youtube`, `google_ads`, `tiktok`, `pinterest`, `bing`, `amazon`, `twitch`, `dv`.
# MAGIC
# MAGIC **Comparação com o Dia Anterior**:
# MAGIC    - Verifica o custo do dia anterior
# MAGIC
# MAGIC **Validações de Qualidade**:
# MAGIC    - Identifica inconsistências nos custos:
# MAGIC      - **VALOR ZERADO**: Quando o custo do dia atual é zero ou `NULL`.
# MAGIC      - **VALOR NEGATIVO (ERRO)**: Quando o custo do dia atual é negativo.
# MAGIC      - **ABAIXO DE 5%**: Quando o custo do dia atual é menor que 5% do custo do dia anterior.
# MAGIC      - **CRESCIMENTO ANORMAL >150%**: Quando o custo do dia atual é maior que 150% do custo do dia anterior.
# MAGIC      - **OK**: Quando não há problemas detectados.
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Observações
# MAGIC - Certifique-se de que a coluna `custo` na tabela especificada seja compatível com a conversão para `FLOAT64`.
# MAGIC

# COMMAND ----------


def qa_table_investimento(dataset, table, start_date, end_date, name_column_date):

    query = f"""
                WITH dados_diarios AS (
                SELECT
                    DATE({name_column_date}) AS data,
                    plataforma,
                    SUM(CAST(custo AS FLOAT64)) AS total_custo
                FROM {dataset}.{table}
                WHERE {name_column_date} BETWEEN DATE_SUB('{start_date}', INTERVAL 1 DAY) AND '{end_date}'
                AND plataforma IN ('facebook', 'twitter', 'youtube', 'google_ads', 'tiktok', 'pinterest', 'bing', 'amazon', 'twitch', 'dv')
                GROUP BY ALL
                ), 

                comparacao AS ( 
                    SELECT
                        a.data,
                        a.plataforma,
                        a.total_custo,
                        LAG(a.total_custo) OVER (PARTITION BY a.plataforma ORDER BY a.data) AS total_custo_dia_anterior
                    FROM dados_diarios a
                )

                -- Verifica validações e ajusta o dia atual
                SELECT
                    data AS `data`,
                    '{dataset}' AS `dataset`,
                    '{table}' AS `table`,
                    plataforma AS `plataforma`,
                    ROUND(total_custo, 2) AS `total_custo`,
                    ROUND(total_custo_dia_anterior, 2) AS `total_custo_dia_anterior`,
                    CASE
                        WHEN total_custo_dia_anterior IS NOT NULL THEN 
                        ROUND(((total_custo - total_custo_dia_anterior) / total_custo_dia_anterior) * 100, 2)
                        ELSE NULL
                    END AS `diferenca_percentual`,
                    CASE
                        WHEN total_custo = 0 OR total_custo is NULL THEN 'VALOR ZERADO'  -- VERIFICA SE O VALOR ESTA ZERADO OU É NULL
                        WHEN total_custo < 0 THEN 'VALOR NEGATIVO (ERRO)'  -- VERIFICA SE CUSTO ESTA NEGATIVO
                        WHEN total_custo_dia_anterior IS NOT NULL AND total_custo < (total_custo_dia_anterior * 0.05) THEN 'ABAIXO DE 5%'  -- VERIFICA SE O CUSTO ESTA ABAIXO DOS 5% DO DIA ANTERIOR
                        WHEN total_custo_dia_anterior IS NOT NULL AND total_custo > (total_custo_dia_anterior * 1.5) THEN 'CRESCIMENTO ANORMAL >150%' -- VERIFICA SE O CUSTO ESTA ACIMA DOS 150% DO DIA ANTERIOR
                        ELSE 'OK'
                    END AS `status`
                FROM comparacao
                WHERE data >= '{start_date}'
                ORDER BY 1
    """
    # print(query)
    return bq_query_df(query)



# COMMAND ----------

# MAGIC %md
# MAGIC ## `insert_log`
# MAGIC
# MAGIC - Realiza uma operação MERGE para atualizar ou inserir registros em uma tabela de log.
# MAGIC ---
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC **Colunas Manipuladas**:
# MAGIC    - As colunas atualizadas ou inseridas incluem: <br> `data`, `dataset`, `table`, `plataforma`, `total_custo`, `total_custo_dia_anterior`, `diferenca_percentual`, `qtd`, `status_date`, `status`, `datas_faltantes` e `created_at`.
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ### Parâmetros
# MAGIC
# MAGIC A função não recebe parâmetros diretamente, mas utiliza:
# MAGIC - `LOG_TABLE`: Nome da tabela de log destino.
# MAGIC - `global_temp.logs_table_investimentos`: Tabela temporária global usada como fonte dos dados.
# MAGIC
# MAGIC <br>

# COMMAND ----------

def insert_log():

    merge_sql = f"""
        MERGE INTO {LOG_TABLE} AS t
        USING global_temp.logs_table_investimentos AS source
        ON t.data = source.data AND t.dataset = source.dataset AND t.table = source.table AND t.plataforma = source.plataforma
        WHEN MATCHED THEN
            UPDATE SET
                t.plataforma = source.plataforma,
                t.total_custo = source.total_custo,
                t.total_custo_dia_anterior = source.total_custo_dia_anterior,
                t.diferenca_percentual = source.diferenca_percentual,
                t.qtd = source.qtd,
                t.status_date = source.status_date,
                t.status = source.status,
                t.datas_faltantes = source.datas_faltantes,
                t.created_at = source.created_at
        WHEN NOT MATCHED THEN
            INSERT (data, dataset, table, plataforma, total_custo, total_custo_dia_anterior, diferenca_percentual, qtd, status_date, status, datas_faltantes, created_at)
            VALUES (source.data, source.dataset, source.table, source.plataforma, source.total_custo, source.total_custo_dia_anterior, source.diferenca_percentual, source.qtd, source.status_date, source.status, source.datas_faltantes, source.created_at)"""
    log_df = spark.sql(merge_sql)
    return log_df


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Descrição
# MAGIC CHAMANDO AS FUNÇÕES ACIMA:
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Parâmetros
# MAGIC
# MAGIC | Parâmetro            | Tipo     | Descrição                                                                 |
# MAGIC |----------------------|----------|---------------------------------------------------------------------------|
# MAGIC | `name_column_date`   | `str`    | Nome da coluna que contém as datas.                                      |
# MAGIC | `dataset`            | `str`    | Nome do dataset que contém a tabela a ser processada.                    |
# MAGIC | `tabela`             | `str`    | Nome da tabela que será utilizada no processamento.                      |
# MAGIC | `start_date`         | `str`    | Data inicial do intervalo a ser analisado (formato `YYYY-MM-DD`).        |
# MAGIC | `end_date`           | `str`    | Data final do intervalo a ser analisado (formato `YYYY-MM-DD`).          |
# MAGIC | `reprocessamento`    | `bool`   | Indica se o script deve operar no modo de reprocessamento.               |
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lógica Interna
# MAGIC
# MAGIC #### Modo de Reprocessamento (`reprocessamento=True`)
# MAGIC Cria uma variável `created_at` com a data e hora atual.
# MAGIC Itera sobre cada dia no intervalo definido por `start_date` e `end_date`.
# MAGIC Para cada dia:
# MAGIC    - Chama a função `logs_datas_faltantes` para obter as datas ausentes.
# MAGIC    - Chama a função `qa_table_investimento` para processar os dados de investimentos.
# MAGIC    - Realiza um `join` entre os resultados das funções.
# MAGIC    - Adiciona a coluna `created_at` ao DataFrame resultante.
# MAGIC    - Acumula os resultados em `df_final`.
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC #### Modo Sem Reprocessamento (`reprocessamento=False`)
# MAGIC Cria uma variável `created_at` com a data e hora atual.
# MAGIC Obtém as datas ausentes e os dados de investimentos para todo o intervalo usando `logs_datas_faltantes` e `qa_table_investimento`.
# MAGIC Realiza um `join` entre os resultados das funções.
# MAGIC Adiciona a coluna `created_at` ao DataFrame resultante.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC #### Criação da Tabela Temporária
# MAGIC - Remove a tabela temporária existente, se houver, com o comando `DROP TABLE`.
# MAGIC - Cria ou substitui a tabela temporária `global_temp.logs_table_investimentos` com os dados acumulados.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC #### Inserção dos Dados na Tabela Final
# MAGIC - Chama a função `insert_log` para inserir os dados processados na tabela final.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### OUTPUT
# MAGIC O script não retorna diretamente um valor, mas:
# MAGIC Cria uma tabela temporária `global_temp.logs_table_investimentos` com os dados processados.
# MAGIC Insere os dados na tabela final de logs utilizando a função `insert_log`.
# MAGIC
# MAGIC

# COMMAND ----------

name_column_date =  'data'
dataset = 'arpu_cac'
tabela = 'consolidado'
start_date = START_DATE
end_date = END_DATE
reprocessamento = False


# Criar DataFrame final acumulado
df_final = None


if reprocessamento:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Iterar por cada dia no intervalo
    for current_date in pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'):
        datas = logs_datas_faltantes(dataset, tabela, current_date, current_date, name_column_date, reprocessamento=False)
        investimentos = qa_table_investimento(dataset, tabela, current_date, current_date, name_column_date)
        
        # Realizar o join para o dia atual
        df_temp = datas.join(investimentos, on=['data', 'dataset', 'table'], how='INNER')
        df_temp = df_temp.withColumn("created_at", F.lit(created_at))
        
        # Acumular os resultados
        if df_final is None:
            df_final = df_temp
        else:
            df_final = df_final.union(df_temp)


else:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    datas = logs_datas_faltantes(dataset, tabela, start_date, end_date, name_column_date)
    investimentos = qa_table_investimento(dataset, tabela, start_date, end_date, name_column_date)
    
    # Realizar o join
    df_final = datas.join(investimentos, on=['data', 'dataset', 'table'], how='INNER')
    df_final = df_final.withColumn("created_at", F.lit(created_at))

# Criar a tabela temporária com os dados acumulados
print("Criando tabela temporária: logs_table_investimentos")
spark.sql("DROP TABLE IF EXISTS global_temp.logs_table_investimentos")
df_final.createOrReplaceGlobalTempView("logs_table_investimentos")



# Inserir os dados na tabela final
insert_log()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecp.bq_profiler.logs_investimentos
# MAGIC --WHERE data = (select max(data) from ecp.bq_profiler.logs_investimentos)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # ALERT
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Verifica se existe algo diferente de "OK" na coluna status
# MAGIC
# MAGIC Se sim, o código vai quebrar, e será disparado uma mensagem no slack informando que existe um erro no processo;

# COMMAND ----------

result = spark.sql("""
    SELECT *
    FROM ecp.bq_profiler.logs_investimentos
    WHERE data = '2024-12-10' --(SELECT MAX(data) FROM ecp.bq_profiler.logs_investimentos)
""")

result.display()


if result.filter(result["status"] != "OK").count() > 0:
    raise Exception("ERRO ENCONTRADO")


# COMMAND ----------


