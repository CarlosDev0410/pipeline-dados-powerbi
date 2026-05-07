select
	vgm.nome,
    SUM(vgm.qtdedisponivel) AS quantidade_disponivel,
    l.nome
FROM
    vgerenciarmaterial vgm
left join localarmazenagem l on
	l.cdlocalarmazenagem = vgm.cdlocalarmazenagem 
left join empresa e on
	e.cdpessoa = vgm.cdempresa 
where
 	vgm.cdmaterial in (23006, 15185, 24846, 24847)
 	and vgm.cdlocalarmazenagem in (265, 34)
group by 
	vgm.nome, l.nome



