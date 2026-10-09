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
    last_price,
    _dlt_load_id,
    _dlt_id
from {{ source('data_platform_catalog', 'instruments') }}
