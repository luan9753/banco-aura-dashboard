# Banco_Aura

Script para calcular o gap entre:

- `sync_orders + sync_items` (chave de sync)
- `orders + order_items` (chave do Aura principal)

Indicadores principais (sempre reportados):

- Pedidos inseridos (`orders`)
- Dispositivos inseridos (`order_items`)
- Pedidos sem relatorio
- Dispositivos sem relatorio

## Regra da chave

- Sync key: `sync_orders.order_code + sync_items.device_serial`
- Aura key: `orders.order_code + order_items.item_label`
- Normalizacao: maiusculo e remocao de caracteres fora de `[A-Z0-9]`

Exemplo: `541010` + `A1010` = `541010A1010`

## Filtro de data

O filtro e aplicado em `sync_orders.delivery_date`:

- `delivery_date > cutoff_date`

Default: `2025-12-04`

## Script

Arquivo: `aura_sync_gap_report.py`

## Uso rapido

```bash
py .\Banco_Aura\aura_sync_gap_report.py
```

## Parametros uteis

```bash
py .\Banco_Aura\aura_sync_gap_report.py --cutoff-date 2025-12-04 --sample-size 20
py .\Banco_Aura\aura_sync_gap_report.py --export-missing-csv .\Banco_Aura\missing_keys.csv
```

## Variaveis de ambiente opcionais

- `AURA_DB_HOST`
- `AURA_DB_NAME`
- `AURA_DB_USER`
- `AURA_DB_PASSWORD`
- `AURA_DB_PORT`
- `AURA_CUTOFF_DATE`
