# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## VISUALIZAÇÃO BANCO DE DADOS PUBLICO VENDAS ECOMMERCE OLIST NO BRASIL
# MAGIC
# MAGIC foi usado tecnicas de analise de dados em SQL e marchine learning para criação visual.
# MAGIC CONSULTAS EM SQL PARA AVALIAR PONTOS IMPORTANTE E ANALISE PREDITIVA DOS DADOS.
# MAGIC UMA CONSULTA EM CTE PARA RESUMIR O TEMPO MÉDIO DA LOGISTICA DA LOJA

# COMMAND ----------

file_location = "/FileStore/tables/ecom_olist/brazil_ecom_geospatial-1.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC CRIAÇÃO DO DATA BASE PARA ANÁLISE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table df_olist
# MAGIC using csv
# MAGIC options (path '/FileStore/tables/ecom_olist/brazil_ecom_geospatial-1.csv', header='true', inferSchema='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_vendas
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC * ANALISE DE SOMA DE TOTAL DE PEDIDOS POR ESTADO

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select customer_state,
# MAGIC  sum(order_id) AS total_orders
# MAGIC FROM df_olist
# MAGIC GROUP BY customer_state limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * VALOR MÉDIO DE GASTO POR PEDIDO

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC AVG(total_payment_value) AS avg_order_value
# MAGIC FROM df_olist;

# COMMAND ----------

# MAGIC %md
# MAGIC * TEMPO MÉDIO DE ENTREGA POR ESTADO

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select customer_state, 
# MAGIC     AVG(customer_delivery_time_fast) AS avg_delivery_time
# MAGIC from df_olist
# MAGIC group by customer_state;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC TIPO DE PAGAMENTO MAIS UTILIZADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select payment_type,
# MAGIC       sum(order_id) AS total_orders
# MAGIC from df_olist
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY total_orders;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CORRELAÇÃO ENTRE O VALOR DO FRETE E PESO DO PRODUTO

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_weight_g, freight_value
# MAGIC from df_olist
# MAGIC order by freight_value desc;

# COMMAND ----------

# MAGIC %md
# MAGIC VENDEDORES COM MAIOR ATRASO NO DESPACHO

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select seller_dispatch_time_delay,
# MAGIC        sum(order_id) AS delayed_orders
# MAGIC  FROM df_olist
# MAGIC  group by seller_dispatch_time_delay 
# MAGIC  order by delayed_orders desc;
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CTE para calcular o tempo médio de entrega por estado e, em seguida, mostrar os estados com seus respectivos tempos de entrega.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     customer_state,
# MAGIC     AVG(customer_delivery_time_fast) AS avg_delivery_time
# MAGIC FROM df_olist
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY avg_delivery_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTE para calcular a média de tempo de entrega por estado
# MAGIC WITH delivery_stats AS (
# MAGIC     SELECT
# MAGIC         customer_state,
# MAGIC         AVG(customer_delivery_time_fast) AS avg_delivery_time
# MAGIC     FROM df_olist
# MAGIC     GROUP BY customer_state
# MAGIC )
# MAGIC -- Consulta final
# MAGIC SELECT
# MAGIC     customer_state,
# MAGIC     avg_delivery_time
# MAGIC FROM delivery_stats
# MAGIC ORDER BY avg_delivery_time DESC;
# MAGIC
