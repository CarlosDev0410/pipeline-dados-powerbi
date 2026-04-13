WITH campos_extra AS (
    SELECT 
        MAX(CASE WHEN nome ILIKE '%FRETE TEMPERARE%' THEN cdcampoextrapedidovendatipo END) as cd_frete,
        MAX(CASE WHEN nome ILIKE '%VIA TRAFEGO?%' THEN cdcampoextrapedidovendatipo END) as cd_trafego,
        MAX(CASE WHEN nome ILIKE '%CLIENTE PROSPECTADO?%' THEN cdcampoextrapedidovendatipo END) as cd_prop
    FROM CAMPOEXTRAPEDIDOVENDATIPO
    WHERE nome ILIKE ANY (ARRAY['%FRETE TEMPERARE%', '%VIA TRAFEGO?%', '%CLIENTE PROSPECTADO?%'])
)
SELECT
    N.dtemissao as DATA_FATURAMENTO,
    PV.DTPEDIDOVENDA as DATA_PEDIDO,
    COALESCE(p.nome, pvt.descricao, 'VENDA DIRETA') as PARCEIRO,
    MG.NOME as GRUPO_MATERIAL,
    M.IDENTIFICACAO,
    M.NOME,
    ((NFPI.qtde) * (NFPI.valorunitario) - COALESCE(NFPI.valordesconto, 0)/ 100)::numeric as VALOR,
    (((NFPI.qtde) * (NFPI.valorunitario) - COALESCE(NFPI.valordesconto, 0)/ 100)/ NFPI.qtde)::numeric as VALOR_UNITARIO,
    COALESCE(SUM(vm.outrasdespesas / 100), 0)::numeric as OUTRAS_DESPESAS,
    PE.nomefantasia as EMPRESA,
    VM.valorcustomaterial::numeric as CMV,
    M.valorvendaminimo::numeric as CUSTO,
    1 as PEDIDOS,
    (SELECT SUM(NFPI2.qtde)::INT FROM notafiscalprodutoitem nfpi2 
     WHERE nfpi2.cdnotafiscalproduto = nfpi.cdnotafiscalproduto AND nfpi2.cdmaterial = nfpi.cdmaterial) as QTDE_VENDIDA,
    N.numero::VARCHAR as NOTA,
    T.nome as TRANSPORTADOR,
    COALESCE((NFPI.valorfrete / 100), 0)::numeric as FRETE,
    CASE
        WHEN vce_frete.VALOR ~ '^[0-9]+([,.][0-9]+)?$'
        THEN replace(replace(vce_frete.VALOR, '.', ''), ',', '.')::numeric
        ELSE 0
    END as FRETE_RAW,
    vce_trafego.VALOR as VIA_TRAFEGO,
    vce_prop.VALOR as CLIENTE_PROP,
    UF.sigla as UF,
    MUN.nome as CIDADE,
    E.bairro as BAIRRO,
    C.NOME as CLIENTE,
    F.nome as FABRICANTE,
    FP.nome as FORMA_PAGAMENTO,
    pp.nome as PRAZO_PAGAMENTO,
    vv.total_documento / 100 as VALORVENDA,
    (CASE
        WHEN c2.contribuinteicmstipo = 0 THEN 'CONTRIBUINTE'
        WHEN c2.contribuinteicmstipo = 1 THEN 'ISENTO'
        WHEN c2.contribuinteicmstipo = 2 THEN 'NÃO CONTRIBUINTE'
        ELSE 'CONTRIBUINTE'
    END) as CONTRIBUINTE_ICMS,
    (CASE
        WHEN NFP.localdestinonfe = 0 THEN 'INTERNA'
        WHEN NFP.localdestinonfe = 1 THEN 'INTERESTADUAL'
        WHEN NFP.localdestinonfe = 2 THEN 'OPERAÇÃO COM EXTERIOR'
    END) as LOCAL_DESTINO,
    COALESCE(NFPi.valoricmsdestinatario, 0)/ 100 as DIFAL,
    COALESCE(SUM(axd.valortermo), 0) / 100 as VALOR_TERMO,
    COUNT(nfpi.cdnotafiscalprodutoitem) as QUANTIDADE,
    VEND.NOME as VENDEDOR,
    PV.identificador::VARCHAR as ID_PEDIDO,
    V.dtaltera as DTALTERA
FROM MATERIAL M
CROSS JOIN campos_extra
JOIN NOTAFISCALPRODUTOITEM NFPI ON (m.cdmaterial = NFPI.cdmaterial)
JOIN materialgrupo mg ON (mg.cdmaterIalgrupo = m.cdmaterialgrupo)
JOIN NOTA N ON (N.CDNOTA = NFPI.cdnotafiscalproduto)
JOIN NOTAFISCALPRODUTO NFP ON (NFP.CDNOTA = N.CDNOTA)
JOIN vwsaidafiscal VNF ON (VNF.cdnota = N.cdnota)
JOIN PESSOA C ON (C.cdpessoa = N.cdcliente)
JOIN cliente c2 ON c2.cdpessoa = n.cdcliente
JOIN EMPRESA PE ON (PE.cdpessoa = N.cdempresa)
JOIN materialfornecedor MF ON (MF.CDMATERIAL = M.cdmaterial AND MF.principal is true)
JOIN pessoa F ON (F.CDPESSOA = MF.cdpessoa)
JOIN ENDERECO E ON (E.cdendereco = N.cdendereco)
LEFT JOIN notavenda NV ON (NV.CDnota = n.cdnota)
LEFT JOIN VENDA V ON (V.CDVENDA = nv.CDVENDA)
LEFT JOIN CLIENTEVENDEDOR CV ON (CV.cdcliente = V.cdcliente AND CV.PRINCIPAL is true)
LEFT JOIN PESSOA VEND ON (VEND.cdpessoa = CV.cdcolaborador)
LEFT JOIN vvalorvenda vv ON (vv.cdvenda = v.cdvenda)
LEFT JOIN vendapagamento vp ON ( vp.cdvenda = v.cdvenda )
LEFT JOIN aux_documento axd ON (axd.cddocumento = vp.cddocumento)
LEFT JOIN vendamaterial VM ON (vm.cdvendamaterial = nfpi.cdvendamaterial )
LEFT JOIN PEDIDOVENDA PV ON (V.CDPEDIDOVENDA = PV.CDPEDIDOVENDA)
LEFT JOIN pessoa p ON (p.cdpessoa = pv.cdparceiro)
LEFT JOIN municipio MUN ON (MUN.cdmunicipio = E.cdmunicipio)
LEFT JOIN UF ON (UF.CDUF = MUN.CDUF)
LEFT JOIN documentotipo FP ON (V.cdformapagamento = FP.cddocumentotipo)
LEFT JOIN prazopagamento pp ON (v.cdprazopagamento = pp.cdprazopagamento)
LEFT JOIN PESSOA T ON (T.CDPESSOA = NFP.cdtransportador)
LEFT JOIN VENDAVALORCAMPOEXTRA vce_frete ON vce_frete.cdvenda = V.cdvenda AND vce_frete.cdcampoextrapedidovendatipo = campos_extra.cd_frete
LEFT JOIN VENDAVALORCAMPOEXTRA vce_trafego ON vce_trafego.cdvenda = V.cdvenda AND vce_trafego.cdcampoextrapedidovendatipo = campos_extra.cd_trafego
LEFT JOIN VENDAVALORCAMPOEXTRA vce_prop ON vce_prop.cdvenda = V.cdvenda AND vce_prop.cdcampoextrapedidovendatipo = campos_extra.cd_prop
LEFT JOIN pedidovendatipo pvt ON (pvt.cdpedidovendatipo = v.cdpedidovendatipo AND pvt.cdpedidovendatipo = 100)
WHERE N.dtemissao >= :data_inicio
  AND (N.dtemissao >= CURRENT_DATE - INTERVAL '1 day' OR V.dtaltera >= :data_ref)
  AND NFPI.valorunitario > 0
  AND N.cdnotastatus IN (10, 11)
  AND ((NFP.cdnaturezaoperacao IN (15, 29, 30, 124, 685)) OR (NFP.cdnaturezaoperacao IS NULL))
GROUP BY
    N.dtemissao, PV.DTPEDIDOVENDA, p.nome, pvt.descricao, MG.NOME, M.IDENTIFICACAO, M.NOME, 
    NFPI.qtde, NFPI.valorunitario, NFPI.valordesconto, PE.nomefantasia, VM.valorcustomaterial, 
    M.valorvendaminimo, n.numero, T.nome, NFPI.valorfrete, vce_frete.VALOR, vce_trafego.VALOR, 
    vce_prop.VALOR, UF.sigla, MUN.nome, E.bairro, C.NOME, F.nome, FP.nome, pp.nome, 
    vv.total_documento, c2.contribuinteicmstipo, NFP.localdestinonfe, NFPi.valoricmsdestinatario, 
    nfpi.cdnotafiscalproduto, nfpi.cdmaterial, VEND.NOME, PV.identificador, V.dtaltera;
