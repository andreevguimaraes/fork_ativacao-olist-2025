-- Databricks notebook source
-- MAGIC %md
-- MAGIC Todas varivaveis com m-1, m-2, m-3 ....
-- MAGIC
-- MAGIC
-- MAGIC Ok - RecenciaPedidos = qtde de dias desde o último pedido?
-- MAGIC
-- MAGIC Ok - TempoDesdePrimeiroPedido = qtde de dias desde o primeiro pedido
-- MAGIC
-- MAGIC FrequenciaConsistencia ???
-- MAGIC
-- MAGIC Ok - TicketMedio = (Frete + Preco) médio?
-- MAGIC
-- MAGIC Ok - QtdPedido
-- MAGIC
-- MAGIC Ok - vlFreteMedia = frete médio por pedido?
-- MAGIC
-- MAGIC Ok - vlPrecoMedia = preço médio por pedido?
-- MAGIC
-- MAGIC VariabilidadeFrequenciaPedidos ???
-- MAGIC
-- MAGIC TaxaDeCrescimentoPedidos ???
-- MAGIC
-- MAGIC Ok - TempoMédioEntrePedidos
-- MAGIC
-- MAGIC avgPedidoMes3
-- MAGIC
-- MAGIC A média de preço por pedido do vendedor = TicketMedio?
-- MAGIC
-- MAGIC Ok - teve pedido ou não nos últimos 6 meses
-- MAGIC
-- MAGIC Número de meses sem vendas nos últimos 12 meses
-- MAGIC
-- MAGIC Ok - Intervalo (dias) entre as datas do primeiro e último pedido
-- MAGIC
-- MAGIC Ok - qtd_itens_ate_hoje
-- MAGIC
-- MAGIC Indicador de tendência de venda nos últimos 3 meses (crescente, estável ou decrescente)
-- MAGIC
-- MAGIC Ok - Faturamento (GMV) - já está na fs_pagamento
-- MAGIC
-- MAGIC Ok - % de aprovação pedido
-- MAGIC
-- MAGIC Meses seguidos com pedido
-- MAGIC
-- MAGIC Ok - Média de dias entre a data de pedido e a estimativa de entrega
-- MAGIC
-- MAGIC Ok - Total de Frete
-- MAGIC
-- MAGIC Ok - Frete./VlProduto (índice médio)
-- MAGIC
-- MAGIC Ok - % de entrega no prazo
-- MAGIC
-- MAGIC Ok - % pedidos parcelados
-- MAGIC
-- MAGIC Quantos dias o vendedor precisa, em média, para fazer uma venda - TempoMédioEntrePedidos?

-- COMMAND ----------

WITH

tb_base AS (
  SELECT  '2017-06-01' AS dtRef,
          v.idVendedor AS idVendedor,
          ip.idPedido AS idPedido,
          DATE(p.dtPedido) AS dtPedido,
          DATE(p.dtAprovado) AS dtAprovacao,
          DATE(p.dtEstimativaEntrega) AS dtEstimativaEntrega,
          DATE(dtEntregue) AS dtEntrega,
          pp.descTipoPagamento AS descTipoPagamento,
          pp.nrParcelas AS nrParcelas,
          SUM(ip.vlPreco) AS vlPreco,
          SUM(ip.vlFrete) AS vlFrete,
          COUNT(ip.idPedidoItem) AS qtdeItensPedido

  FROM    silver.olist.vendedor as v

  LEFT JOIN silver.olist.item_pedido AS ip
  ON v.idVendedor = ip.idVendedor

  LEFT JOIN silver.olist.pedido AS p
  ON ip.idPedido = p.idPedido

  LEFT JOIN silver.olist.pagamento_pedido AS pp
  ON ip.idPedido = pp.idPedido

  WHERE p.dtPedido < '2017-06-01'
  GROUP BY ALL
),

-- Incluir os atributos da feature store pagamento na tabela
tb_feat_vendas AS (
  SELECT  dtRef,
          idVendedor,
          -- Recência Pedidos: qtde de dias desde o último pedido
          DATE_DIFF(dtRef, MAX(dtPedido)) AS qtdeDiasUltimoPedido,
          -- Tempo desde primeiro pedido: qtde de dias desde o primeiro pedido
          DATE_DIFF(dtRef, MIN(dtPedido)) AS qtdeDiasPrimeiroPedido,
          COUNT(idPedido) AS qtdePedidos,
          AVG(vlPreco + vlFrete) AS vlTicketMedio,
          AVG(vlPreco) AS vlMediaPreco,
          AVG(vlFrete) AS vlMediaFrete,
          MAX(dtPedido) >= DATE('2017-06-01') - INTERVAL 6 MONTH AS inPedido6Meses,
          DATE_DIFF(MAX(dtPedido), MIN(dtPedido)) AS qtdeDiasPrimeiroUltimoPedido,
          SUM(COALESCE(qtdeItensPedido, 0)) AS qtdeItensPedido,
          SUM(CASE WHEN dtAprovacao IS NULL THEN 0 ELSE 1 END) / COUNT(idPedido) AS pctPedidosAprovados,
          AVG(DATE_DIFF(dtEstimativaEntrega, dtPedido)) AS qtdeMediaDiasPedidoEstEntrega,
          SUM(vlFrete) AS vlTotalFrete,
          SUM(vlFrete) / SUM(vlPreco) AS indFretePreco,
          AVG(vlFrete / vlPreco) AS indMedioFretePrecoPorPedido,
          SUM(CASE WHEN DATE_DIFF(dtEstimativaEntrega, dtEntrega) >= 0 THEN 1 ELSE 0 END) / COUNT(dtEntrega) AS pctPedidosEntreguesNoPrazo,
          SUM(CASE WHEN COALESCE(nrParcelas, 1) > 1 THEN 1 ELSE 0 END) / COUNT(idPedido) AS pctPedidosParcelados,
        COUNT(CASE WHEN dtPedido >= '2017-06-01' - INTERVAL 28 DAY THEN idPedido END) AS qtdePedidoD28,
        COUNT(CASE WHEN dtPedido >= '2017-06-01' - INTERVAL 14 DAY THEN idPedido END) AS qtdePedidoD14,
        COUNT(CASE WHEN dtPedido >= '2017-06-01' - INTERVAL 7 DAY THEN idPedido END) AS qtdePedidoD7,
        COUNT(CASE WHEN dtPedido >= '2017-06-01' - INTERVAL 28 DAY THEN idPedido END) / COUNT(CASE WHEN dtPedido >= '2017-06-01' - INTERVAL 56 DAY AND dtPedido < '2017-06-01' - INTERVAL 28 DAY THEN idPedido END) AS crescimentoD28,
        count(distinct CASE WHEN dtPedido >= '2017-06-01' - interval 84 DAY THEN idPedido END) / 3 AS avgPedidoM3
  FROM    tb_base
  GROUP BY dtRef,
          idVendedor
),

tb_daily AS (
  SELECT  DISTINCT
          idVendedor,
          date(dtPedido) AS dtPedido
  FROM    tb_base
  GROUP BY ALL
),

tb_lag AS (
  SELECT  *,
          LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido DESC) AS dtProximoPedido
  FROM    tb_daily
),

tb_feat_vendas_lag AS (
  SELECT  idVendedor,
          AVG(DATE_DIFF(tb_lag.dtProximoPedido, tb_lag.dtPedido)) AS qtdeMediaDiasEntrePedidos
  FROM    tb_lag
  GROUP BY idVendedor
),

tb_weekly AS (

SELECT idVendedor,
        year(dtPedido) || weekofyear(dtPedido) AS dtWeek,
        count(distinct idPedido) AS qtdePedidoSemana

FROM tb_base
GROUP BY ALL
),

summary_weekly AS (
SELECT idVendedor,
       stddev_pop(qtdePedidoSemana) AS stdPedidoSemana
FROM tb_weekly
GROUP BY ALL

),

tb_final AS (

SELECT  t1.*,
        t2.qtdeMediaDiasEntrePedidos,
        t3.stdPedidoSemana
FROM tb_feat_vendas AS t1

LEFT JOIN tb_feat_vendas_lag AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN summary_weekly as t3
ON t1.idVendedor = t3.idVendedor

)

SELECT *
FROM tb_final
