select
	vgm.nome,
    vgm.qtdedisponivel AS quantidade_disponivel
FROM
    vgerenciarmaterial vgm
where
 	vgm.cdmaterial in (23006)
 	and vgm.cdlocalarmazenagem = 265
