-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS eth_txs_history_eth_tx_id_id_idx
ON eth_txs_history (eth_tx_id, id DESC);