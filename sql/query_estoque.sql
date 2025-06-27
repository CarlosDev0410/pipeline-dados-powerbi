SELECT
    l.nome AS "Local de Armazenagem",
    vgm.identificacao AS "Identificador",
    vgm.nome AS "Material",
    F.nome as Fabricante,
    mg.nome AS "Grupo de Material",
    vgm.qtdedisponivel AS "Quantidade Disponível",
    vgm.valorcustomaterial AS "Valor Unitário",
    (vgm.qtdedisponivel * vgm.valorcustomaterial) AS "Valor Total"
FROM
    vgerenciarmaterial vgm
JOIN
    material m ON vgm.cdmaterial = m.cdmaterial
LEFT JOIN
    materialgrupo mg ON vgm.cdmaterialgrupo = mg.cdmaterialgrupo
JOIN
    localarmazenagem l ON vgm.cdlocalarmazenagem = l.cdlocalarmazenagem
JOIN
	MATERIALFORNECEDOR MF ON MF.cdmaterial = M.cdmaterial AND MF.principal IS true
JOIN
	PESSOA F ON F.cdpessoa = MF.cdpessoa
WHERE
    m.produto IS TRUE
    AND m.ativo IS TRUE
    AND vgm.cdlocalarmazenagem IN (34, 67, 265, 463, 133, 331);