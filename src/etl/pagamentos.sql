-- Databricks notebook source
with tb_pagamento as (
SELECT

  pag.*,
  item.idVendedor
  
FROM silver.olist.pedido ped

left join silver.olist.pagamento_pedido pag
  on ped.idPedido = pag.idPedido
  
left join silver.olist.item_pedido item
  on ped.idPedido = item.idPedido

where 1=1
  and ped.dtPedido::date < '2018-01-01'
  and ped.dtPedido::date >= add_months('2018-01-01', -6)
  and item.idVendedor is not null
)
, tb_group as (
SELECT
  idVendedor,
  descTipoPagamento,
  count(distinct idPedido) as qtd_pedidos,
  sum(vlPagamento) as valor_total
FROM tb_pagamento
group by 1, 2
order by 1, 2
)

select
  idVendedor,
  
  sum(case when descTipoPagamento = 'boleto' then qtd_pedidos else 0 end) as qtd_pedidos_boleto,
  sum(case when descTipoPagamento = 'credit_card' then qtd_pedidos else 0 end) as qtd_pedidos_cc,
  sum(case when descTipoPagamento = 'voucher' then qtd_pedidos else 0 end) as qtd_pedidos_voucher,
  sum(case when descTipoPagamento = 'debit_card' then qtd_pedidos else 0 end) as qtd_pedidos_debit,
  
  sum(case when descTipoPagamento = 'boleto' then valor_total else 0 end) as valor_total_boleto,
  sum(case when descTipoPagamento = 'credit_card' then valor_total else 0 end) as valor_total_cc,
  sum(case when descTipoPagamento = 'voucher' then valor_total else 0 end) as valor_total_voucher,
  sum(case when descTipoPagamento = 'debit_card' then valor_total else 0 end) as valor_total_debit,
  
  sum(case when descTipoPagamento = 'boleto' then qtd_pedidos else 0 end) / sum(qtd_pedidos) as pct_qtd_pedidos_boleto,
  sum(case when descTipoPagamento = 'credit_card' then qtd_pedidos else 0 end)/ sum(qtd_pedidos) as pct_qtd_pedidos_cc,
  sum(case when descTipoPagamento = 'voucher' then qtd_pedidos else 0 end)/ sum(qtd_pedidos) as pct_qtd_pedidos_voucher,
  sum(case when descTipoPagamento = 'debit_card' then qtd_pedidos else 0 end)/ sum(qtd_pedidos) as pct_qtd_pedidos_debit,
  
  sum(case when descTipoPagamento = 'boleto' then valor_total else 0 end) / sum(valor_total) as pct_valor_total_boleto,
  sum(case when descTipoPagamento = 'credit_card' then valor_total else 0 end) / sum(valor_total) as pct_valor_total_cc,
  sum(case when descTipoPagamento = 'voucher' then valor_total else 0 end) / sum(valor_total) as pct_valor_total_voucher,
  sum(case when descTipoPagamento = 'debit_card' then valor_total else 0 end) / sum(valor_total) as pct_valor_total_debit
  
from tb_group
group by 1

-- COMMAND ----------


