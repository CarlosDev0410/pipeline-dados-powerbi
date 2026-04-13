WITH campos_extra AS (
    SELECT 
        MAX(CASE WHEN nome ILIKE '%FRETE%' THEN cdcampoextrapedidovendatipo END) as cd_frete
    FROM CAMPOEXTRAPEDIDOVENDATIPO
    WHERE nome ILIKE '%FRETE%'
)
SELECT
    N.dtemissao,
    PV.DTPEDIDOVENDA,
    CASE
        WHEN NFP.intermediadornfe = 0 THEN 'SEM INTERMEDIADOR'
        WHEN NFP.intermediadornfe = 1 THEN p5.nome
        ELSE 'SEM INDICATIVO DE INTERMEDIADOR INFORMADO'
    END AS parceiro,
    MG.NOME AS GRUPO_MATERIAL,
    M.IDENTIFICACAO,
    M.NOME,
    SUM((NFPI.qtde * NFPI.valorunitario) - COALESCE(NFPI.valordesconto, 0) / 100)::numeric AS VALOR,
    SUM(((NFPI.qtde * NFPI.valorunitario) - COALESCE(NFPI.valordesconto, 0) / 100) / NFPI.qtde)::numeric AS VALOR_UNITARIO,
    (NFP.outrasdespesas / 100)::numeric AS OUTRAS_DESPESAS,
    M.valorcusto::numeric AS cmv,
    M.valorvendaminimo::numeric AS custo,
    COUNT(1) AS PEDIDOS,
    SUM(NFPI.qtde)::INT AS QTDE_VENDIDA,
    N.numero AS nota,
    T.nome AS TRANSPORTADOR,
    SUM(((NFPI.qtde * NFPI.valorunitario) - COALESCE(NFPI.valordesconto, 0) / 100)
        * (VNF.valorfrete / 100 / (VNF.valortotalprodutos + 0.001)))::numeric AS FRETE,
    SUM(
        (NFPI.qtde * (NFPI.valorunitario - COALESCE(NFPI.valordesconto, 0) / 100)
        * (100 - COALESCE(V.percentualdesconto, 0)))
        / (NFP.valor - VNF.valorfrete + 0.001)
        * (REPLACE(REPLACE(COALESCE(VCE.VALOR, '0'), '.', ''), ',', '.')::numeric / 100)
        * 100
    )::numeric AS FRETE_TEMPERARE,
    UF.sigla AS UF,
    MUN.nome AS CIDADE,
    E.bairro,
    C.NOME AS CLIENTE,
    F.nome AS FABRICANTE,
    FP.nome AS FORMA_PAGAMENTO,
    PP.nome AS PRAZO_PAGAMENTO,
    NFP.numeropedido AS ID_PEDIDO,
    CASE
        WHEN N.cdempresa = 3 THEN 'TEMPERARE'
        WHEN N.cdempresa = 11531 THEN 'TEMPERARE EQUIPAMENTOS'
        WHEN N.cdempresa = 75997 THEN 'TEMPERARE ESPIRITO SANTO'
    END AS empresa
FROM MATERIAL M
CROSS JOIN campos_extra
JOIN NOTAFISCALPRODUTOITEM NFPI ON M.cdmaterial = NFPI.cdmaterial
JOIN MATERIALGRUPO MG ON MG.cdmaterialgrupo = M.cdmaterialgrupo
JOIN NOTA N ON N.cdnota = NFPI.cdnotafiscalproduto
JOIN NOTAFISCALPRODUTO NFP ON NFP.cdnota = N.cdnota
JOIN VWSAIDAFISCAL VNF ON VNF.cdnota = N.cdnota
JOIN PESSOA C ON C.cdpessoa = N.cdcliente
JOIN MATERIALFORNECEDOR MF ON MF.cdmaterial = M.cdmaterial AND MF.principal IS TRUE
JOIN PESSOA F ON F.cdpessoa = MF.cdpessoa
JOIN ENDERECO E ON E.cdendereco = N.cdendereco
LEFT JOIN NOTAVENDA NV ON NV.cdnota = N.cdnota
LEFT JOIN VENDA V ON V.cdvenda = NV.cdvenda
LEFT JOIN VENDAMATERIAL VM ON VM.cdvenda = V.cdvenda AND VM.cdvendamaterial = NFPI.cdvendamaterial
LEFT JOIN PEDIDOVENDA PV ON PV.cdpedidovenda = V.cdpedidovenda
LEFT JOIN PESSOA P ON P.cdpessoa = PV.cdparceiro
LEFT JOIN MUNICIPIO MUN ON MUN.cdmunicipio = E.cdmunicipio
LEFT JOIN UF ON UF.cduf = MUN.cduf
LEFT JOIN FORMAPAGAMENTO FP ON FP.cdformapagamento = V.cdformapagamento
LEFT JOIN PRAZOPAGAMENTO PP ON PP.cdprazopagamento = V.cdprazopagamento
LEFT JOIN PESSOA T ON T.cdpessoa = NFP.cdtransportador
LEFT JOIN PESSOA P5 ON P5.cdpessoa = NFP.cdintermediador
LEFT JOIN VENDAVALORCAMPOEXTRA VCE ON VCE.cdvenda = V.cdvenda AND VCE.cdcampoextrapedidovendatipo = campos_extra.cd_frete
LEFT JOIN PEDIDOVENDATIPO PVT ON PVT.cdpedidovendatipo = V.cdpedidovendatipo AND PVT.cdpedidovendatipo = 100
WHERE
    N.dtemissao = :dataref
    AND NFPI.valorunitario > 0
    AND N.cdnotastatus IN (10, 11)
    AND N.cdprojeto <> 34
    AND NFP.cdnaturezaoperacao = 19
GROUP BY
    N.dtemissao, PV.DTPEDIDOVENDA, NFP.intermediadornfe, p5.nome, MG.NOME, M.IDENTIFICACAO, M.NOME, 
    NFP.outrasdespesas, M.valorcusto, M.valorvendaminimo, N.numero, T.nome, UF.sigla, MUN.nome, 
    E.bairro, C.NOME, F.nome, FP.nome, PP.nome, NFP.numeropedido, N.cdempresa, campos_extra.cd_frete;
