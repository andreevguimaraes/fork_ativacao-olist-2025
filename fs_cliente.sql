-- Databricks notebook source
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

-- FErnando -- testes

WITH transacoes AS (
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
acum AS (
    SELECT 
        t.ano_mes,
        t.vendedor, 
        t.cliente, 
        t.uf_cliente,
        COUNT(*) AS comprasNoMes,
        SUM(COUNT(*)) OVER (
            PARTITION BY t.vendedor, t.cliente 
            ORDER BY t.ano_mes 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS comprasHistoricas
    FROM transacoes t
    GROUP BY t.ano_mes, t.vendedor, t.cliente, t.uf_cliente
),
classificacao AS (
    SELECT 
        a.ano_mes,
        a.vendedor, 
        a.cliente, 
        a.uf_cliente,
        a.comprasNoMes,
        a.comprasHistoricas,
        CASE 
            WHEN a.comprasNoMes > 0 AND a.comprasHistoricas IS NULL THEN 'Cliente Novo'
            WHEN a.comprasNoMes > 0 AND a.comprasHistoricas >= 2 THEN 'Cliente Recorrente'
            WHEN a.comprasNoMes = 0 AND a.comprasHistoricas > 0 THEN 'Cliente Pontual'
            ELSE 'Outro'
        END AS categoriaCliente
    FROM acum a
),
clientes_estados AS (
    SELECT 
        vendedor,
        uf_cliente,
        COUNT(DISTINCT cliente) AS total_clientes_uf
    FROM transacoes
    GROUP BY vendedor, uf_cliente
),
tb_colunas AS (
    SELECT 
        c.vendedor,
        COUNT(DISTINCT c.cliente) AS total_clientes,
        COUNT(DISTINCT CASE WHEN c.categoriaCliente = 'Cliente Novo' THEN c.cliente END) AS CliNovos,
        COUNT(DISTINCT CASE WHEN c.categoriaCliente = 'Cliente Recorrente' THEN c.cliente END) AS CliRecorr,
        COUNT(DISTINCT CASE WHEN c.categoriaCliente = 'Cliente Pontual' THEN c.cliente END) AS CliPontuais
    FROM classificacao c
    WHERE c.ano_mes = '2017-05'
    GROUP BY c.vendedor
), tb_final as
(
SELECT 
    f.vendedor, 
    f.total_clientes, 
    f.CliNovos, 
    f.CliRecorr, 
    f.CliPontuais,
    e.uf_cliente,
    e.total_clientes_uf
FROM tb_colunas f
LEFT JOIN clientes_estados e ON f.vendedor = e.vendedor
ORDER BY f.vendedor, e.uf_cliente
)
select * from tb_final
limit 10
