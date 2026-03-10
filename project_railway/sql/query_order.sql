select
	pv.cdpedidovenda as CODIGO,
	pv.dtpedidovenda as DATA,
	coalesce(p.nome, 'VENDA MANUAL') as CANAL,
	coalesce(pv.identificador, '') as IDENTIFICADOR,
	coalesce(m.identificacao, 'S/ SKU') as SKU,
	coalesce(m.nome, 'PRODUTO NÃO IDENTIFICADO') as PRODUTO,
	coalesce(pvm.quantidade, 0) as QTD,
	(coalesce(pv.valorfrete, 0) / 100.0) as FRETE,
	(coalesce(pvm.desconto, 0) / 100.0) as DESCONTO,
	(coalesce(pvm.preco, 0) - (coalesce(pvm.desconto, 0) / 100.0)) as VALOR_PRODUTO,
	((coalesce(pv.valorfrete, 0) / 100.0) + (coalesce(pvm.preco, 0))) - (coalesce(pvm.desconto, 0) / 100.0) as VALOR_FINAL,
	coalesce(l.nome, 'ESTOQUE PADRÃO') as LOCAL_DE_ESTOQUE,
	coalesce(pvt.descricao, 'OUTROS') as ORIGEM,
	coalesce(pv.pedidovendasituacao, 0) as SITUACAO
from 
	pedidovenda pv
left join pessoa p
	on pv.cdparceiro = p.cdpessoa 
join pedidovendatipo pvt
	on pv.cdpedidovendatipo = pvt.cdpedidovendatipo 
join pedidovendamaterial pvm
	on pv.cdpedidovenda = pvm.cdpedidovenda
join material m
	on pvm.cdmaterial = m.cdmaterial
join localarmazenagem l 
	on pv.cdlocalarmazenagem = l.cdlocalarmazenagem 
where 
	pv.dtpedidovenda >= :data_inicio
	and pv.pedidovendasituacao in (0, 2)
	and pv.cdlocalarmazenagem <> 463
order by
	pv.dtpedidovenda;