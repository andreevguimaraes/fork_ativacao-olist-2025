-- Databricks notebook source
-- ANTERIOR

WITH transacoes AS (
    SELECT  
        v.idVendedor AS vendedor, 
        c.idClienteUnico AS cliente, 
        c.descUF AS UfCliente,
        date_format(p.dtPedido, 'yyyy-MM') AS mesAno
    
    FROM silver.olist.pedido p
    
    INNER JOIN silver.olist.item_pedido i ON i.idPedido = p.idPedido
    INNER JOIN silver.olist.cliente c ON c.idCliente = p.idCliente
    INNER JOIN silver.olist.vendedor v ON v.idVendedor = i.idVendedor
    
    WHERE p.descSituacao <> 'canceled'
    AND p.dtPedido < '2017-06-01'
),

acum AS (
    SELECT 
        t.vendedor, 
        t.cliente, 
        t.UfCliente, 
        t.mesAno, 
        COUNT(*) AS comprasNoMes,
        SUM(COUNT(*)) OVER (PARTITION BY t.vendedor, t.cliente ORDER BY t.mesAno ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS comprasUltimos6Meses
    FROM transacoes t
    GROUP BY t.vendedor, t.cliente, t.UfCliente, t.mesAno
),

classificacao AS (
    SELECT 
        a.vendedor, 
        a.cliente, 
        a.UfCliente, 
        a.mesAno, 
        a.comprasNoMes,
        a.comprasUltimos6Meses,
        CASE 
            WHEN a.comprasNoMes > 0 AND a.comprasUltimos6Meses IS NULL THEN 'Cliente Novo'
            WHEN a.comprasNoMes = 1 AND a.comprasUltimos6Meses = 1 THEN 'Cliente Pontual'
            WHEN a.comprasNoMes > 0 AND a.comprasUltimos6Meses >= 2 THEN 'Cliente Recorrente'
            ELSE 'Outro'
        END AS categoriaCliente
    FROM acum a
)

SELECT 
    distinct 
    '2017-06-01' AS referencia, 
    c.vendedor,

    count(distinct c.cliente) AS qtdCliente,
    count(distinct CASE WHEN c.categoriaCliente = 'Cliente Novo' THEN c.cliente END ) / count(distinct c.cliente) AS cliNovo,
    count(distinct CASE WHEN c.categoriaCliente = 'Cliente Pontual' THEN c.cliente END ) / count(distinct c.cliente) AS cliPontual,
    count(distinct CASE WHEN c.categoriaCliente = 'Cliente Recorrente' THEN c.cliente END ) / count(distinct c.cliente) AS cliRecorrente,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'AC' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfAC,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'AL' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfAL,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'AM' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfAM,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'AP' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfAP,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'BA' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfBA,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'CE' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfCE,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'DF' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfDF,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'ES' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfES,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'GO' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfGO,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'MA' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfMA,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'MG' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfMG,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'MS' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfMS,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'MT' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfMT,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'PA' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfPA,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'PB' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfPB,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'PE' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfPE,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'PI' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfPI,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'PR' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfPR,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'RJ' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfRJ,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'RN' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfRN,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'RO' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfRO,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'RR' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfRR,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'RS' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfRS,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'SC' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfSC,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'SE' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfSE,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'SP' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfSP,
    COUNT(DISTINCT CASE WHEN c.UfCLiente = 'TO' THEN c.Cliente END) / count(distinct c.cliente) AS cliUfTO

FROM classificacao c

GROUP BY ALL

-- COMMAND ----------

-- PROPOSTA

with lista_vendedores (
        SELECT  
        distinct
        v.idVendedor AS vendedor, 
        c.idClienteUnico AS cliente
    FROM silver.olist.pedido p
    JOIN silver.olist.item_pedido i ON i.idPedido = p.idPedido
    JOIN silver.olist.cliente c ON c.idCliente = p.idCliente
    JOIN silver.olist.vendedor v ON v.idVendedor = i.idVendedor
    WHERE p.descSituacao <> 'canceled'
    AND p.dtPedido < '2017-06-01'
), transacoes AS (
    SELECT  
        date_format(p.dtPedido, 'yyyy-MM') AS ano_mes,
        v.idVendedor AS vendedor, 
        c.idClienteUnico AS cliente,
        c.descUF AS uf_cliente
    FROM silver.olist.pedido p
    JOIN silver.olist.item_pedido i ON i.idPedido = p.idPedido
    JOIN silver.olist.cliente c ON c.idCliente = p.idCliente
    JOIN silver.olist.vendedor v ON v.idVendedor = i.idVendedor
    WHERE p.descSituacao <> 'canceled'
    AND p.dtPedido < '2017-06-01'
),
classificacao AS (
    SELECT 
        a.vendedor, 
        a.cliente, 
        sum(case when b.ano_mes = '2017-05' then 1 else 0 end) as comprasNoMes,
        sum(case when b.ano_mes < '2017-05' then 1 else 0 end) as comprasNohistorico                                  
    FROM lista_vendedores a
        left join transacoes b
            on a.vendedor = b.vendedor 
                AND a.cliente = b.cliente
    --where b.ano_mes > '2017-05' - interval 360 days
    group by
        a.vendedor, 
        a.cliente
),
clientes_estados AS (
    SELECT 
        vendedor,
        uf_cliente,
        COUNT(DISTINCT cliente) AS total_clientes_uf
    FROM transacoes
    GROUP BY vendedor, uf_cliente
), agrup_classificacao as
(
select 
    vendedor, 
    cliente,     
    sum(
        case 
            when comprasNoMes > 0 and nvl(comprasNohistorico,0) = 0 
                then 1 else 0 end) flg_novo,
    sum(
        case 
            when comprasNoMes > 0 and nvl(comprasNohistorico,0) > 0 
                then 1 else 0 end) flg_recorrente,
    sum(
        case 
            when nvl(comprasNoMes,0) = 0 and nvl(comprasNohistorico,0) > 0 
                then 1 else 0 end) flg_pontual     
from classificacao
group by all
),
tbCatClientes as
(
select
    vendedor idVendedor,
    count(cliente) qtd_clientes,
    --sum(flg_novo) cliNovo_QTD,
    --sum(flg_recorrente) cliRecorrente_QTD,
    --sum(flg_pontual) cliPontual_QTD,
    sum(flg_novo) / count(cliente) cliNovo,
    sum(flg_recorrente) / count(cliente) cliRecorrente,
    sum(flg_pontual) / count(cliente) cliPontual
from agrup_classificacao
group by vendedor
)
select 
    A.*,
    sum(b.total_clientes_uf) cli,
    -- SUDESTE
    SUM(case when b.uf_cliente = 'SP' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfSP,
    SUM(case when b.uf_cliente = 'RJ' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfRJ,
    SUM(case when b.uf_cliente = 'ES' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfES,
    SUM(case when b.uf_cliente = 'MG' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfMG,

    -- SUL
    SUM(case when b.uf_cliente = 'RS' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfRS,
    SUM(case when b.uf_cliente = 'SC' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfSC,
    SUM(case when b.uf_cliente = 'PR' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfPR,

    -- CENTRO OESTE
    SUM(case when b.uf_cliente = 'DF' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfDF,
    SUM(case when b.uf_cliente = 'GO' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfGO,
    SUM(case when b.uf_cliente = 'MS' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfMS,
    SUM(case when b.uf_cliente = 'MT' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfMT,

    -- NORDESTE
    SUM(case when b.uf_cliente = 'BA' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfBA,
    SUM(case when b.uf_cliente = 'CE' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfCE,
    SUM(case when b.uf_cliente = 'AL' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfAL,
    SUM(case when b.uf_cliente = 'MA' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfMA,
    SUM(case when b.uf_cliente = 'PB' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfPB,
    SUM(case when b.uf_cliente = 'PE' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfPE,
    SUM(case when b.uf_cliente = 'PI' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfPI,
    SUM(case when b.uf_cliente = 'SE' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfSE,
    SUM(case when b.uf_cliente = 'RN' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfRN,

    -- NORTE
    SUM(case when b.uf_cliente = 'AC' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfAC,
    SUM(case when b.uf_cliente = 'AM' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfAM,
    SUM(case when b.uf_cliente = 'AP' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfAP,
    SUM(case when b.uf_cliente = 'PA' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfPA,
    SUM(case when b.uf_cliente = 'RO' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfRO,
    SUM(case when b.uf_cliente = 'RR' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfRR,
    SUM(case when b.uf_cliente = 'TO' then total_clientes_uf else 0 end) / sum(b.total_clientes_uf) as cliUfTO
    
from tbCatClientes a
    left join  clientes_estados b
        on a.idVendedor = b.vendedor
--WHERE idVendedor = 'b1fecf4da1fa2689bccffa0121953643'
GROUP BY ALL

-- COMMAND ----------


-- validacao
/*
    SELECT  
        date_format(p.dtPedido, 'yyyy-MM') AS ano_mes,
        v.idVendedor AS vendedor, 
        c.idClienteUnico AS cliente,
        c.descUF AS uf_cliente
    FROM silver.olist.pedido p
    JOIN silver.olist.item_pedido i ON i.idPedido = p.idPedido
    JOIN silver.olist.cliente c ON c.idCliente = p.idCliente
    JOIN silver.olist.vendedor v ON v.idVendedor = i.idVendedor
    WHERE p.descSituacao <> 'canceled'
    AND p.dtPedido < '2017-06-01'
    and v.idVendedor = 'b1fecf4da1fa2689bccffa0121953643'
*/
  
