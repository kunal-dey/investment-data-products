select
    instrument_token,
    exchange_token,
    tradingsymbol,
    name,
    exchange,
    segment,
    instrument_type,
    expiry,
    strike,
    lot_size,
    tick_size,
    last_price
from {{ source('data_platform_catalog', 'instruments') }}
