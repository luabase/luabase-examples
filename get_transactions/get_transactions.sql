select
t.block_timestamp,
t.block_number,
t.block_hash,
t.transaction_index,
t.hash,
t.from_address,
t.to_address,
t.value,
t.gas,
t.gas_price,
t.input,
t.nonce

from ethereum.transactions as t
where {{sync_date_param}} 