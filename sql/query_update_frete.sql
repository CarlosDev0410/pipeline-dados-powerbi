select
    PV.identificador::VARCHAR as ID_PEDIDO,
    case
        when vce_frete.VALOR ~ '^[0-9]+([,.][0-9]+)?$'
        then replace(replace(vce_frete.VALOR, '.', ''), ',', '.')::numeric
        else 0
    end as FRETE_TOTAL,
    V.dtaltera as DTALTERA
from
    VENDA V
join PEDIDOVENDA PV on (V.cdpedidovenda  = PV.cdpedidovenda)
left join VENDAVALORCAMPOEXTRA vce_frete on
    vce_frete.cdvenda = V.cdvenda
    and vce_frete.cdcampoextrapedidovendatipo in (
        select cdcampoextrapedidovendatipo
        from CAMPOEXTRAPEDIDOVENDATIPO
        where nome ilike '%FRETE TEMPERARE%'
    )
where
    V.dtaltera >= :data_ref
    and V.dtvenda >= CURRENT_DATE - INTERVAL '60 days'
group by
    1, 2, 3
