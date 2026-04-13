WITH campos_extra AS (
    SELECT 
        MAX(CASE WHEN nome ILIKE '%FRETE TEMPERARE%' THEN cdcampoextrapedidovendatipo END) as cd_frete,
        MAX(CASE WHEN nome ILIKE '%VIA TRAFEGO?%' THEN cdcampoextrapedidovendatipo END) as cd_trafego,
        MAX(CASE WHEN nome ILIKE '%CLIENTE PROSPECTADO?%' THEN cdcampoextrapedidovendatipo END) as cd_prop
    FROM CAMPOEXTRAPEDIDOVENDATIPO
    WHERE nome ILIKE ANY (ARRAY['%FRETE TEMPERARE%', '%VIA TRAFEGO?%', '%CLIENTE PROSPECTADO?%'])
)
SELECT
    V.dtvenda as DATA_FATURAMENTO,
    PV.DTPEDIDOVENDA as DATA_PEDIDO,
    COALESCE(p.nome, pvt.descricao, 'VENDA DIRETA') as PARCEIRO,
    MG.NOME as GRUPO_MATERIAL,
    M.IDENTIFICACAO,
    M.NOME,
    SUM(((VM.quantidade) * (VM.PRECO) - COALESCE(vm.desconto, 0)/ 100))::numeric as VALOR,
    SUM((((VM.quantidade) * (VM.PRECO) - COALESCE(vm.desconto, 0)/ 100))/ VM.quantidade)::numeric as VALOR_UNITARIO,
    SUM(vm.outrasdespesas / 100)::numeric as OUTRAS_DESPESAS,
    PE.nomefantasia as EMPRESA,
    VM.valorcustomaterial::numeric as CMV,
    M.valorvendaminimo::numeric as CUSTO,
    1 as PEDIDOS,
    null::int as QTDE_VENDIDA,
    V.cdvenda::VARCHAR as NOTA,
    T.nome as TRANSPORTADOR,
    (V.valorfrete / 100)::numeric as FRETE,
    COALESCE(replace(vce_frete.VALOR, ',', '.'), '0')::numeric as FRETE_RAW,
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
    null as LOCAL_DESTINO,
    null::numeric as DIFAL,
    0::numeric as VALOR_TERMO,
    COUNT(vm.cdvendamaterial) as QUANTIDADE,
    VEND.NOME as VENDEDOR,
    PV.identificador::VARCHAR as ID_PEDIDO,
    V.dtaltera as DTALTERA
FROM MATERIAL M
CROSS JOIN campos_extra
JOIN vendamaterial VM ON (m.cdmaterial = VM.cdmaterial)
JOIN venda V ON (V.cdvenda = VM.cdvenda)
LEFT JOIN CLIENTEVENDEDOR CV ON (CV.cdcliente = V.cdcliente AND CV.PRINCIPAL is true)
LEFT JOIN PESSOA VEND ON (VEND.cdpessoa = CV.cdcolaborador)
LEFT JOIN vvalorvenda vv ON (vv.cdvenda = v.cdvenda)
JOIN materialgrupo mg ON (mg.cdmaterIalgrupo = m.cdmaterialgrupo)
JOIN PESSOA C ON (C.cdpessoa = V.cdcliente)
JOIN cliente c2 ON c2.cdpessoa = v.cdcliente
JOIN EMPRESA PE ON (PE.cdpessoa = V.cdempresa)
LEFT JOIN materialfornecedor MF ON (MF.CDMATERIAL = M.cdmaterial AND MF.principal is true)
JOIN pessoa F ON (F.CDPESSOA = MF.cdpessoa)
JOIN ENDERECO E ON (E.cdendereco = v.cdenderecoentrega)
LEFT JOIN pedidovendatipo pvt ON (pvt.cdpedidovendatipo = v.cdpedidovendatipo AND pvt.cdpedidovendatipo = 100)
LEFT JOIN PEDIDOVENDA PV ON (V.CDPEDIDOVENDA = PV.CDPEDIDOVENDA)
LEFT JOIN vvalorpedidovenda vp ON (vp.cdpedidovenda = pv.cdpedidovenda)
LEFT JOIN pessoa p ON (p.cdpessoa = pv.cdparceiro)
LEFT JOIN municipio MUN ON (MUN.cdmunicipio = E.cdmunicipio)
LEFT JOIN UF ON (UF.CDUF = MUN.CDUF)
LEFT JOIN documentotipo FP ON (V.cdformapagamento = FP.cddocumentotipo)
LEFT JOIN prazopagamento pp ON (v.cdprazopagamento = pp.cdprazopagamento)
LEFT JOIN PESSOA T ON (T.CDPESSOA = v.cdterceiro)
LEFT JOIN VENDAVALORCAMPOEXTRA vce_frete ON vce_frete.cdvenda = V.cdvenda AND vce_frete.cdcampoextrapedidovendatipo = campos_extra.cd_frete
LEFT JOIN VENDAVALORCAMPOEXTRA vce_trafego ON vce_trafego.cdvenda = V.cdvenda AND vce_trafego.cdcampoextrapedidovendatipo = campos_extra.cd_trafego
LEFT JOIN VENDAVALORCAMPOEXTRA vce_prop ON vce_prop.cdvenda = V.cdvenda AND vce_prop.cdcampoextrapedidovendatipo = campos_extra.cd_prop
WHERE V.dtvenda >= :data_inicio
  AND (V.dtvenda >= CURRENT_DATE - INTERVAL '1 day' OR V.dtaltera >= :data_ref)
  AND v.cdvendasituacao = 5
  AND v.cdprojeto = 67
GROUP BY
    V.dtvenda, PV.DTPEDIDOVENDA, p.nome, pvt.descricao, MG.NOME, M.IDENTIFICACAO, M.NOME, 
    PE.nomefantasia, VM.valorcustomaterial, M.valorvendaminimo, V.cdvenda, T.nome, V.valorfrete, 
    vce_frete.VALOR, vce_trafego.VALOR, vce_prop.VALOR, UF.sigla, MUN.nome, E.bairro, C.NOME, 
    F.nome, FP.nome, vv.total_documento, c2.contribuinteicmstipo, pp.nome, VEND.NOME, 
    PV.identificador, V.dtaltera;
