select
	distinct
	DATA_FATURAMENTO,
	DATA_PEDIDO,
	PARCEIRO,
	GRUPO_MATERIAL,
	IDENTIFICACAO,
	NOME,
	VALOR,
	VALOR_UNITARIO,
	OUTRAS_DESPESAS,
	EMPRESA,
	cmv,
	CUSTO,
	PEDIDOS,
	QTDE_VENDIDA,
	NOTA,
	TRANSPORTADOR,
	FRETE,
	(FRETE_TEMPERARE / QUANTIDADE) as FRETE_TEMPERARE,
	UF,
	CIDADE,
	BAIRRO,
	CLIENTE,
	FABRICANTE,
	FORMA_PAGAMENTO,
	PRAZO_PAGAMENTO,
	CONTRIBUINTE_ICMS,
	LOCAL_DESTINO,
	DIFAL,
	(case
		when QUANTIDADE <= 1 then VALOR_TERMO
		when QUANTIDADE > 1 then VALOR_TERMO / QUANTIDADE
	end) as COMISSAO_DO_CANAL,
	VENDEDOR,
	VIA_TRAFEGO,
	CLIENTE_PROP,
	ID_PEDIDO
from
	(
	select
		N.dtemissao as DATA_FATURAMENTO,
		PV.DTPEDIDOVENDA as DATA_PEDIDO,
		coalesce(p.nome, pvt.descricao, 'VENDA DIRETA') as parceiro,
		MG.NOME as GRUPO_MATERIAL,
		M.IDENTIFICACAO,
		M.NOME,
		((NFPI.qtde) * (NFPI.valorunitario) - coalesce(NFPI.valordesconto, 0)/ 100) as VALOR,
		(((NFPI.qtde) * (NFPI.valorunitario) - coalesce(NFPI.valordesconto, 0)/ 100)/ NFPI.qtde) as VALOR_UNITARIO,
		coalesce(sum(vm.outrasdespesas / 100), 0) as OUTRAS_DESPESAS,
		PE.nomefantasia as empresa,
		vm.valorcustomaterial as cmv,
		m.valorvendaminimo as custo,
		1 as PEDIDOS,
		(
		select
			SUM(NFPI2.qtde)::INT
		from
			notafiscalprodutoitem nfpi2
		where
			nfpi2.cdnotafiscalproduto = nfpi.cdnotafiscalproduto
			and nfpi2.cdmaterial = nfpi.cdmaterial)as QTDE_VENDIDA,
		n.numero as nota,
		T.nome as TRANSPORTADOR,
		coalesce((NFPI.valorfrete / 100), 0) as FRETE,
		case
			when vce_frete.VALOR ~ '^[0-9]+([,.][0-9]+)?$'
    		then replace(replace(vce_frete.VALOR, '.', ''), ',', '.')::numeric
			else 0
		end as FRETE_TEMPERARE,
		vce_trafego.VALOR as VIA_TRAFEGO,
		vce_prop.VALOR as CLIENTE_PROP,
		UF.sigla as UF,
		MUN.nome as CIDADE,
		E.bairro,
		C.NOME as CLIENTE,
		F.nome as FABRICANTE,
		FP.nome as FORMA_PAGAMENTO,
		pp.nome as prazo_pagamento,
		vv.total_documento / 100 as valorvenda,
		(case
			when c2.contribuinteicmstipo = 0 then 'CONTRIBUINTE'
			when contribuinteicmstipo = 1 then 'ISENTO'
			when contribuinteicmstipo = 2 then 'NÃO CONTRIBUINTE'
			else 'CONTRIBUINTE'
		end) as CONTRIBUINTE_ICMS,
		(case
			when NFP.localdestinonfe = 0 then 'INTERNA'
			when localdestinonfe = 1 then 'INTERESTADUAL'
			when localdestinonfe = 2 then 'OPERAÇÃO COM EXTERIOR'
		end) as LOCAL_DESTINO,
		coalesce(NFPi.valoricmsdestinatario, 0)/ 100 as DIFAL,
		coalesce(sum(axd.valortermo), 0) / 100 as VALOR_TERMO,
		count(nfpi.cdnotafiscalprodutoitem) as QUANTIDADE,
		VEND.NOME as VENDEDOR,
		PV.identificador::VARCHAR as ID_PEDIDO
	from
		MATERIAL M
	join NOTAFISCALPRODUTOITEM NFPI on
		(m.cdmaterial = NFPI.cdmaterial)
	join materialgrupo mg on
		(mg.cdmaterIalgrupo = m.cdmaterialgrupo)
	join NOTA N on
		(N.CDNOTA = NFPI.cdnotafiscalproduto)
	join NOTAFISCALPRODUTO NFP on
		(NFP.CDNOTA = N.CDNOTA)
	join vwsaidafiscal VNF on
		(VNF.cdnota = N.cdnota)
	join PESSOA C on
		(C.cdpessoa = N.cdcliente)
	join cliente c2 on
		c2.cdpessoa = n.cdcliente
	join EMPRESA PE on
		(PE.cdpessoa = N.cdempresa)
	join materialfornecedor MF on
		(MF.CDMATERIAL = M.cdmaterial
			and MF.principal is true)
	join pessoa F on
		(F.CDPESSOA = MF.cdpessoa)
	join ENDERECO E on
		(E.cdendereco = N.cdendereco)
	left join notavenda NV on
		(NV.CDnota = n.cdnota)
	left join VENDA V on
		(V.CDVENDA = nv.CDVENDA)
	left join CLIENTEVENDEDOR CV on
		(CV.cdcliente = V.cdcliente
			and CV.PRINCIPAL is true)
	left join PESSOA VEND on
		(VEND.cdpessoa = CV.cdcolaborador)
	left join vvalorvenda vv on
		(vv.cdvenda = v.cdvenda)
	left join vendapagamento vp on
		( vp.cdvenda = v.cdvenda )
	left join aux_documento axd on
		(axd.cddocumento = vp.cddocumento)
	left join vendamaterial VM on
		(vm.cdvendamaterial = nfpi.cdvendamaterial )
	left join PEDIDOVENDA PV on
		(V.CDPEDIDOVENDA = PV.CDPEDIDOVENDA)
	left join pessoa p on
		(p.cdpessoa = pv.cdparceiro)
	left join municipio MUN on
		(MUN.cdmunicipio = E.cdmunicipio)
	left join UF on
		(UF.CDUF = MUN.CDUF)
	left join documentotipo FP on
		(V.cdformapagamento = FP.cddocumentotipo)
	left join prazopagamento pp on
		(v.cdprazopagamento = pp.cdprazopagamento)
	left join PESSOA T on
		(T.CDPESSOA = NFP.cdtransportador)
	left join VENDAVALORCAMPOEXTRA vce_frete on
		vce_frete.cdvenda = V.cdvenda
		and vce_frete.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%FRETE TEMPERARE%'
    )
	left join VENDAVALORCAMPOEXTRA vce_trafego on
		vce_trafego.cdvenda = V.cdvenda
		and vce_trafego.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%VIA TRAFEGO?%'
    )
	left join VENDAVALORCAMPOEXTRA vce_prop on
		vce_prop.cdvenda = V.cdvenda
		and vce_prop.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%CLIENTE PROSPECTADO?%'
    )
	left join pedidovendatipo pvt on
		(pvt.cdpedidovendatipo = v.cdpedidovendatipo
			and pvt.cdpedidovendatipo = 100)
	where
		1 = 1
		and N.dtemissao BETWEEN :data_inicio AND :data_fim
		and NFPI.valorunitario > 0
		and N.cdnotastatus in (10, 11)
		and ((NFP.cdnaturezaoperacao in (15, 29, 30, 124, 685))
			or (NFP.cdnaturezaoperacao is null))
	group by
		1,
		2,
		3,
		4,
		5,
		6,
		10,
		nfpi.qtde,
		nfpi.valorunitario,
		nfpi.valordesconto,
		11 ,
		12,
		15,
		16,
		17,
		18,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		FP.nome,
		vv.total_documento,
		c2.contribuinteicmstipo,
		nfp.localdestinonfe,
		nfpi.valoricmsdestinatario,
		pp.nome,
		nfpi.cdnotafiscalproduto,
		nfpi.cdmaterial,
		VEND.NOME,
		PV.identificador
  union
	select
		V.dtvenda as DATA_FATURAMENTO,
		PV.DTPEDIDOVENDA as DATA_PEDIDO,
		coalesce(p.nome, pvt.descricao, 'VENDA DIRETA') as parceiro,
		MG.NOME as GRUPO_MATERIAL,
		M.IDENTIFICACAO,
		M.NOME,
		SUM(((VM.quantidade) * (VM.PRECO) - coalesce(vm.desconto, 0)/ 100)) as VALOR,
		SUM((((VM.quantidade) * (VM.PRECO) - coalesce(vm.desconto, 0)/ 100))/ VM.quantidade) as VALOR_UNITARIO,
		sum(vm.outrasdespesas / 100) as OUTRAS_DESPESAS,
		PE.nomefantasia as empresa,
		vm.valorcustomaterial as cmv,
		m.valorvendaminimo as custo,
		1 as PEDIDOS,
		null as QTDE_VENDIDA,
		v.cdvenda::VARCHAR as nota,
		T.nome as TRANSPORTADOR,
		(v.valorfrete / 100) as FRETE,
		coalesce(replace(vce_frete.VALOR, ',', '.'), '0')::numeric as FRETE_TEMPERARE,
		vce_trafego.VALOR as VIA_TRAFEGO,
		vce_prop.VALOR as CLIENTE_PROP,
		UF.sigla as UF,
		MUN.nome as CIDADE,
		E.bairro,
		C.NOME as CLIENTE,
		F.nome as FABRICANTE,
		FP.nome as FORMA_PAGAMENTO,
		pp.nome as prazo_pagamento,
		vv.total_documento / 100 as valorvenda,
		(case
			when c2.contribuinteicmstipo = 0 then 'CONTRIBUINTE'
			when contribuinteicmstipo = 1 then 'ISENTO'
			when contribuinteicmstipo = 2 then 'NÃO CONTRIBUINTE'
			else 'CONTRIBUINTE'
		end) as CONTRIBUINTE_ICMS,
		null as LOCAL_DESTINO,
		null::numeric as DIFAL,
		null::numeric as VALOR_TERMO,
		count(vm.cdvendamaterial) as QUANTIDADE,
		VEND.NOME as VENDEDOR,
		PV.identificador::VARCHAR as ID_PEDIDO
	from
		MATERIAL M
	join vendamaterial VM on
		(m.cdmaterial = VM.cdmaterial)
	join venda V on
		(V.cdvenda = VM.cdvenda)
	left join CLIENTEVENDEDOR CV on
		(CV.cdcliente = V.cdcliente
			and CV.PRINCIPAL is true)
	left join PESSOA VEND on
		(VEND.cdpessoa = CV.cdcolaborador)
	left join vvalorvenda vv on
		(vv.cdvenda = v.cdvenda)
	join materialgrupo mg on
		(mg.cdmaterIalgrupo = m.cdmaterialgrupo)
	join PESSOA C on
		(C.cdpessoa = V.cdcliente)
	join cliente c2 on
		c2.cdpessoa = v.cdcliente
	join EMPRESA PE on
		(PE.cdpessoa = V.cdempresa)
	left join materialfornecedor MF on
		(MF.CDMATERIAL = M.cdmaterial
			and MF.principal is true)
	join pessoa F on
		(F.CDPESSOA = MF.cdpessoa)
	join ENDERECO E on
		(E.cdendereco = v.cdenderecoentrega)
	left join pedidovendatipo pvt on
		(pvt.cdpedidovendatipo = v.cdpedidovendatipo
			and pvt.cdpedidovendatipo = 100)
	left join PEDIDOVENDA PV on
		(V.CDPEDIDOVENDA = PV.CDPEDIDOVENDA)
	left join vvalorpedidovenda vp on
		(vp.cdpedidovenda = pv.cdpedidovenda)
	left join pessoa p on
		(p.cdpessoa = pv.cdparceiro)
	left join municipio MUN on
		(MUN.cdmunicipio = E.cdmunicipio)
	left join UF on
		(UF.CDUF = MUN.CDUF)
	left join documentotipo FP on
		(V.cdformapagamento = FP.cddocumentotipo)
	left join prazopagamento pp on
		(v.cdprazopagamento = pp.cdprazopagamento)
	left join PESSOA T on
		(T.CDPESSOA = v.cdterceiro)
	left join VENDAVALORCAMPOEXTRA vce_frete on
		vce_frete.cdvenda = V.cdvenda
		and vce_frete.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%FRETE TEMPERARE%'
)
	left join VENDAVALORCAMPOEXTRA vce_trafego on
		vce_trafego.cdvenda = V.cdvenda
		and vce_trafego.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%VIA TRAFEGO?%'
)
	left join VENDAVALORCAMPOEXTRA vce_prop on
		vce_prop.cdvenda = V.cdvenda
		and vce_prop.cdcampoextrapedidovendatipo in (
		select
			cdcampoextrapedidovendatipo
		from
			CAMPOEXTRAPEDIDOVENDATIPO
		where
			nome ilike '%CLIENTE PROSPECTADO?%'
)
	where
		1 = 1
		and V.dtvenda BETWEEN :data_inicio AND :data_fim
		and v.cdvendasituacao = 5
		and v.cdprojeto = 67
	group by
		1,
		2,
		3,
		4,
		5,
		6,
		10,
		11,
		12,
		15,
		16,
		17,
		18,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		FP.nome,
		vv.total_documento,
		vp.total_documento,
		c2.contribuinteicmstipo,
		pp.nome,
		VEND.NOME,
		PV.identificador
	order by
		1,
		8 desc
) as t
