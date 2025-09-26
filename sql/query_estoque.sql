SELECT
    l.nome AS local_armazenagem,
    vgm.identificacao AS identificador,
    vgm.nome AS material,
    F.nome AS fabricante,
    mg.nome AS grupo_material,
    SUM(vgm.qtdedisponivel) AS quantidade_disponivel,
    vgm.valorcustomaterial AS valor_unitario,
    SUM(vgm.qtdedisponivel * vgm.valorcustomaterial) AS valor_total
FROM
    vgerenciarmaterial vgm
JOIN
    material m ON vgm.cdmaterial = m.cdmaterial
LEFT JOIN
    materialgrupo mg ON vgm.cdmaterialgrupo = mg.cdmaterialgrupo
JOIN
    localarmazenagem l ON vgm.cdlocalarmazenagem = l.cdlocalarmazenagem
JOIN
    MATERIALFORNECEDOR MF ON MF.cdmaterial = M.cdmaterial AND MF.principal IS TRUE
JOIN
    PESSOA F ON F.cdpessoa = MF.cdpessoa
WHERE
    m.produto IS TRUE
    AND m.ativo IS TRUE
    AND vgm.cdlocalarmazenagem IN (34, 67, 265, 463, 133, 331, 300, 331, 397, 499, 500, 501)
GROUP BY
    l.nome, vgm.identificacao, vgm.nome, F.nome, mg.nome, vgm.valorcustomaterial;
