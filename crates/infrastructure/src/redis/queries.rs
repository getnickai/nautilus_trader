// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{collections::HashMap, str::FromStr, sync::Arc};

use ahash::AHashMap;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use nautilus_common::{cache::database::CacheMap, enums::SerializationEncoding};
use nautilus_model::{
    accounts::AccountAny,
    data::{CustomData, DataType, HasTsInit},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, PositionId},
    instruments::{InstrumentAny, SyntheticInstrument},
    orders::OrderAny,
    position::Position,
    types::Currency,
};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use ustr::Ustr;

use super::get_index_key;

// Collection keys
const INDEX: &str = "index";
const GENERAL: &str = "general";
const CURRENCIES: &str = "currencies";
const INSTRUMENTS: &str = "instruments";
const SYNTHETICS: &str = "synthetics";
const ACCOUNTS: &str = "accounts";
const ORDERS: &str = "orders";
const POSITIONS: &str = "positions";
const ACTORS: &str = "actors";
const STRATEGIES: &str = "strategies";
const CUSTOM: &str = "custom";
const REDIS_DELIMITER: char = ':';

// Index keys
const INDEX_ORDER_IDS: &str = "index:order_ids";
const INDEX_ORDER_POSITION: &str = "index:order_position";
const INDEX_ORDER_CLIENT: &str = "index:order_client";
const INDEX_ORDERS: &str = "index:orders";
const INDEX_ORDERS_OPEN: &str = "index:orders_open";
const INDEX_ORDERS_CLOSED: &str = "index:orders_closed";
const INDEX_ORDERS_EMULATED: &str = "index:orders_emulated";
const INDEX_ORDERS_INFLIGHT: &str = "index:orders_inflight";
const INDEX_POSITIONS: &str = "index:positions";
const INDEX_POSITIONS_OPEN: &str = "index:positions_open";
const INDEX_POSITIONS_CLOSED: &str = "index:positions_closed";

#[derive(Debug)]
pub struct DatabaseQueries;

/// Max concurrent per-key Redis reads when hydrating collections (orders, instruments, etc.).
/// Unbounded concurrent loads can overload a single Redis connection and hit `response_timeout`.
const REDIS_KEY_LOAD_CONCURRENCY: usize = 64;

impl DatabaseQueries {
    /// Strips `{trader_key}:{collection}:` from a full Redis key returned by [`Self::scan_keys`].
    ///
    /// The remainder is the record identifier. Identifiers may contain `:` (for example
    /// [`InstrumentId`] and [`PositionId`]); they must not be parsed with [`str::rsplit`].
    fn record_id_from_scanned_key<'a>(
        full_key: &'a str,
        trader_key: &str,
        collection: &str,
    ) -> Option<&'a str> {
        let mut prefix = String::with_capacity(trader_key.len() + collection.len() + 3);
        prefix.push_str(trader_key);
        prefix.push(REDIS_DELIMITER);
        prefix.push_str(collection);
        prefix.push(REDIS_DELIMITER);
        full_key.strip_prefix(&prefix)
    }

    /// Serializes the given `payload` using the specified `encoding` to a byte vector.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization to the chosen encoding fails.
    pub fn serialize_payload<T: Serialize>(
        encoding: SerializationEncoding,
        payload: &T,
    ) -> anyhow::Result<Vec<u8>> {
        let mut value = serde_json::to_value(payload)?;
        convert_timestamps(&mut value);
        match encoding {
            SerializationEncoding::MsgPack => rmp_serde::to_vec(&value)
                .map_err(|e| anyhow::anyhow!("Failed to serialize msgpack `payload`: {e}")),
            SerializationEncoding::Json => serde_json::to_vec(&value)
                .map_err(|e| anyhow::anyhow!("Failed to serialize json `payload`: {e}")),
        }
    }

    /// Deserializes the given byte slice `payload` into type `T` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization from the chosen encoding fails or converting to the target type fails.
    pub fn deserialize_payload<T: DeserializeOwned>(
        encoding: SerializationEncoding,
        payload: &[u8],
    ) -> anyhow::Result<T> {
        let mut value = match encoding {
            SerializationEncoding::MsgPack => rmp_serde::from_slice(payload)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize msgpack `payload`: {e}"))?,
            SerializationEncoding::Json => serde_json::from_slice(payload)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize json `payload`: {e}"))?,
        };

        convert_timestamp_strings(&mut value);

        serde_json::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to convert value to target type: {e}"))
    }

    /// Scans Redis for keys matching the given `pattern`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis scan operation fails.
    pub async fn scan_keys(
        con: &mut ConnectionManager,
        pattern: String,
    ) -> anyhow::Result<Vec<String>> {
        let mut result = Vec::new();
        let mut cursor = 0u64;

        loop {
            let scan_result: (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(5000)
                .query_async(con)
                .await?;

            let (new_cursor, keys) = scan_result;
            result.extend(keys);

            // If cursor is 0, we've completed the full scan
            if new_cursor == 0 {
                break;
            }

            cursor = new_cursor;
        }

        Ok(result)
    }

    /// Bulk reads multiple keys from Redis using MGET for efficiency.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis MGET operation fails.
    pub async fn read_bulk(
        con: &ConnectionManager,
        keys: &[String],
    ) -> anyhow::Result<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut con = con.clone();

        // Use MGET to fetch all keys in a single network operation
        let results: Vec<Option<Vec<u8>>> =
            redis::cmd("MGET").arg(keys).query_async(&mut con).await?;

        // Convert Vec<u8> to Bytes
        let bytes_results: Vec<Option<Bytes>> = results
            .into_iter()
            .map(|opt| opt.map(Bytes::from))
            .collect();

        Ok(bytes_results)
    }

    /// Bulk reads multiple keys from Redis using MGET, batched into chunks.
    ///
    /// Keys are batched into chunks of `batch_size` to avoid exceeding Redis
    /// request size limits on some providers.
    ///
    /// # Errors
    ///
    /// Returns an error if `batch_size` is zero or if the underlying Redis MGET operation fails.
    pub async fn read_bulk_batched(
        con: &ConnectionManager,
        keys: &[String],
        batch_size: usize,
    ) -> anyhow::Result<Vec<Option<Bytes>>> {
        if batch_size == 0 {
            anyhow::bail!("`batch_size` must be greater than zero");
        }

        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut all_results: Vec<Option<Bytes>> = Vec::with_capacity(keys.len());

        for chunk in keys.chunks(batch_size) {
            let mut con = con.clone();

            let results: Vec<Option<Vec<u8>>> =
                redis::cmd("MGET").arg(chunk).query_async(&mut con).await?;

            all_results.extend(results.into_iter().map(|opt| opt.map(Bytes::from)));
        }

        Ok(all_results)
    }

    /// Reads raw byte payloads for `key` under `trader_key` from Redis.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails or if the collection is unsupported.
    pub async fn read(
        con: &ConnectionManager,
        trader_key: &str,
        key: &str,
    ) -> anyhow::Result<Vec<Bytes>> {
        let collection = Self::get_collection_key(key)?;
        let full_key = format!("{trader_key}{REDIS_DELIMITER}{key}");

        let mut con = con.clone();

        match collection {
            INDEX => Self::read_index(&mut con, &full_key).await,
            GENERAL => Self::read_string(&mut con, &full_key).await,
            CURRENCIES => Self::read_string(&mut con, &full_key).await,
            INSTRUMENTS => Self::read_string(&mut con, &full_key).await,
            SYNTHETICS => Self::read_string(&mut con, &full_key).await,
            ACCOUNTS => Self::read_list(&mut con, &full_key).await,
            ORDERS => Self::read_list(&mut con, &full_key).await,
            POSITIONS => Self::read_list(&mut con, &full_key).await,
            ACTORS => Self::read_string(&mut con, &full_key).await,
            STRATEGIES => Self::read_string(&mut con, &full_key).await,
            _ => anyhow::bail!("Unsupported operation: `read` for collection '{collection}'"),
        }
    }

    /// Loads all cache data (currencies, instruments, synthetics, accounts, orders, positions) for `trader_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if loading any of the individual caches fails or combining data fails.
    pub async fn load_all(
        con: &ConnectionManager,
        encoding: SerializationEncoding,
        trader_key: &str,
    ) -> anyhow::Result<CacheMap> {
        let (currencies, instruments, synthetics, accounts, orders, positions) = tokio::try_join!(
            Self::load_currencies(con, trader_key, encoding),
            Self::load_instruments(con, trader_key, encoding),
            Self::load_synthetics(con, trader_key, encoding),
            Self::load_accounts(con, trader_key, encoding),
            Self::load_orders(con, trader_key, encoding),
            Self::load_positions(con, trader_key, encoding)
        )
        .map_err(|e| anyhow::anyhow!("Error loading cache data: {e}"))?;

        // For now, we don't load greeks and yield curves from the database
        // This will be implemented in the future
        let greeks = AHashMap::new();
        let yield_curves = AHashMap::new();

        Ok(CacheMap {
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        })
    }

    /// Loads all currencies for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading currency data fails.
    pub async fn load_currencies(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<Ustr, Currency>> {
        let mut currencies = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{CURRENCIES}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        if keys.is_empty() {
            return Ok(currencies);
        }

        // Use bulk loading with MGET for efficiency
        let bulk_values = Self::read_bulk(&con, &keys).await?;

        // Process the bulk results
        for (key, value_opt) in keys.iter().zip(bulk_values.iter()) {
            let Some(code) = Self::record_id_from_scanned_key(key.as_str(), trader_key, CURRENCIES)
            else {
                log::error!("Invalid key format: {key}");
                continue;
            };
            let currency_code = Ustr::from(code);

            if let Some(value_bytes) = value_opt {
                match Self::deserialize_payload(encoding, value_bytes) {
                    Ok(currency) => {
                        currencies.insert(currency_code, currency);
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize currency {currency_code}: {e}");
                    }
                }
            } else {
                log::error!("Currency not found in Redis: {currency_code}");
            }
        }

        log::debug!("Loaded {} currencies(s)", currencies.len());

        Ok(currencies)
    }

    /// Loads all instruments for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading instrument data fails.
    /// Loads all instruments for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading instrument data fails.
    pub async fn load_instruments(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<InstrumentId, InstrumentAny>> {
        let mut instruments = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{INSTRUMENTS}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(keys.into_iter().map(|key| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                let Some(code) = Self::record_id_from_scanned_key(key.as_str(), tk.as_ref(), INSTRUMENTS)
                else {
                    log::error!("Invalid key format: {key}");
                    return None;
                };
                let instrument_id = match InstrumentId::from_str(code) {
                    Ok(id) => id,
                    Err(e) => {
                        log::error!("Failed to convert to InstrumentId for {key}: {e}");
                        return None;
                    }
                };

                match Self::load_instrument(&con, tk.as_ref(), &instrument_id, encoding).await {
                    Ok(Some(instrument)) => Some((instrument_id, instrument)),
                    Ok(None) => {
                        log::error!("Instrument not found: {instrument_id}");
                        None
                    }
                    Err(e) => {
                        log::error!("Failed to load instrument {instrument_id}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        instruments.extend(rows.into_iter().flatten());
        log::debug!("Loaded {} instruments(s)", instruments.len());

        Ok(instruments)
    }

    /// Loads all synthetic instruments for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading synthetic instrument data fails.
    /// Loads all synthetic instruments for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading synthetic instrument data fails.
    pub async fn load_synthetics(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<InstrumentId, SyntheticInstrument>> {
        let mut synthetics = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{SYNTHETICS}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(keys.into_iter().map(|key| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                let Some(code) = Self::record_id_from_scanned_key(key.as_str(), tk.as_ref(), SYNTHETICS)
                else {
                    log::error!("Invalid key format: {key}");
                    return None;
                };
                let instrument_id = match InstrumentId::from_str(code) {
                    Ok(id) => id,
                    Err(e) => {
                        log::error!("Failed to parse InstrumentId for {key}: {e}");
                        return None;
                    }
                };

                match Self::load_synthetic(&con, tk.as_ref(), &instrument_id, encoding).await {
                    Ok(Some(synthetic)) => Some((instrument_id, synthetic)),
                    Ok(None) => {
                        log::error!("Synthetic not found: {instrument_id}");
                        None
                    }
                    Err(e) => {
                        log::error!("Failed to load synthetic {instrument_id}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        synthetics.extend(rows.into_iter().flatten());
        log::debug!("Loaded {} synthetics(s)", synthetics.len());

        Ok(synthetics)
    }

    /// Loads all accounts for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading account data fails.
    /// Loads all accounts for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading account data fails.
    pub async fn load_accounts(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<AccountId, AccountAny>> {
        let mut accounts = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{ACCOUNTS}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(keys.into_iter().map(|key| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                let Some(code) = Self::record_id_from_scanned_key(key.as_str(), tk.as_ref(), ACCOUNTS)
                else {
                    log::error!("Invalid key format: {key}");
                    return None;
                };
                let account_id = AccountId::from(code);

                match Self::load_account(&con, tk.as_ref(), &account_id, encoding).await {
                    Ok(Some(account)) => Some((account_id, account)),
                    Ok(None) => {
                        log::error!("Account not found: {account_id}");
                        None
                    }
                    Err(e) => {
                        log::error!("Failed to load account {account_id}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        accounts.extend(rows.into_iter().flatten());
        log::debug!("Loaded {} accounts(s)", accounts.len());

        Ok(accounts)
    }

    /// Loads all orders for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading order data fails.
    /// Loads all orders for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading order data fails.
    pub async fn load_orders(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<ClientOrderId, OrderAny>> {
        let mut orders = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{ORDERS}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(keys.into_iter().map(|key| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                let Some(code) = Self::record_id_from_scanned_key(key.as_str(), tk.as_ref(), ORDERS)
                else {
                    log::error!("Invalid key format: {key}");
                    return None;
                };
                let client_order_id = ClientOrderId::from(code);

                match Self::load_order(&con, tk.as_ref(), &client_order_id, encoding).await {
                    Ok(Some(order)) => Some((client_order_id, order)),
                    Ok(None) => {
                        log::error!("Order not found: {client_order_id}");
                        None
                    }
                    Err(e) => {
                        log::error!("Failed to load order {client_order_id}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        orders.extend(rows.into_iter().flatten());
        log::debug!("Loaded {} order(s)", orders.len());

        Ok(orders)
    }

    /// Loads all positions for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading position data fails.
    /// Loads all positions for `trader_key` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or reading position data fails.
    pub async fn load_positions(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<PositionId, Position>> {
        let mut positions = AHashMap::new();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{POSITIONS}*");
        log::debug!("Loading {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(keys.into_iter().map(|key| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                let Some(code) = Self::record_id_from_scanned_key(key.as_str(), tk.as_ref(), POSITIONS)
                else {
                    log::error!("Invalid key format: {key}");
                    return None;
                };
                let position_id = PositionId::from(code);

                match Self::load_position(&con, tk.as_ref(), &position_id, encoding).await {
                    Ok(Some(position)) => Some((position_id, position)),
                    Ok(None) => {
                        log::error!("Position not found: {position_id}");
                        None
                    }
                    Err(e) => {
                        log::error!("Failed to load position {position_id}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        positions.extend(rows.into_iter().flatten());
        log::debug!("Loaded {} position(s)", positions.len());

        Ok(positions)
    }

    /// Loads all custom data for `trader_key` matching the given `data_type`.
    ///
    /// Keys are stored as `custom:<ts_init_020>:<uuid>`; value is full CustomData JSON.
    /// Scans all custom keys, deserializes, filters by type_name (full or short), metadata,
    /// and identifier to match SQL semantics, then sorts by ts_init ascending.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning, bulk read, or deserialization fails.
    pub async fn load_custom_data(
        con: &ConnectionManager,
        trader_key: &str,
        data_type: &DataType,
    ) -> anyhow::Result<Vec<CustomData>> {
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{CUSTOM}*");
        log::debug!("Loading custom data {pattern}");

        let mut con = con.clone();
        let keys = Self::scan_keys(&mut con, pattern).await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let values = Self::read_bulk(&con, &keys).await?;
        let request_type_name = data_type.type_name();
        let request_short = request_type_name
            .rsplit([':', '.'])
            .next()
            .unwrap_or(request_type_name);
        let request_identifier = data_type.identifier().unwrap_or("");

        let mut results = Vec::new();

        for value_opt in values {
            let Some(value_bytes) = value_opt else {
                continue;
            };
            let custom = match CustomData::from_json_bytes(value_bytes.as_ref()) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("Failed to deserialize custom data from Redis: {e}");
                    continue;
                }
            };
            let stored_type_name = custom.data_type.type_name();
            let type_match =
                stored_type_name == request_type_name || stored_type_name == request_short;
            let identifier_match =
                custom.data_type.identifier().unwrap_or("") == request_identifier;
            let metadata_match = match (data_type.metadata(), custom.data_type.metadata()) {
                (None, None) => true,
                (Some(a), Some(b)) => serde_json::to_value(a).ok() == serde_json::to_value(b).ok(),
                _ => false,
            };

            if type_match && identifier_match && metadata_match {
                results.push(custom);
            }
        }

        results.sort_by_key(|c| c.ts_init());
        log::debug!("Loaded {} custom data item(s)", results.len());
        Ok(results)
    }

    /// Loads a single currency for `trader_key` and `code` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_currency(
        con: &ConnectionManager,
        trader_key: &str,
        code: &Ustr,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<Currency>> {
        let key = format!("{CURRENCIES}{REDIS_DELIMITER}{code}");
        let result = Self::read(con, trader_key, &key).await?;

        if result.is_empty() {
            return Ok(None);
        }

        let currency = Self::deserialize_payload(encoding, &result[0])?;
        Ok(currency)
    }

    /// Loads a single instrument for `trader_key` and `instrument_id` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_instrument(
        con: &ConnectionManager,
        trader_key: &str,
        instrument_id: &InstrumentId,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        let key = format!("{INSTRUMENTS}{REDIS_DELIMITER}{instrument_id}");
        let result = Self::read(con, trader_key, &key).await?;
        if result.is_empty() {
            return Ok(None);
        }

        let instrument: InstrumentAny = Self::deserialize_payload(encoding, &result[0])?;
        Ok(Some(instrument))
    }

    /// Loads a single synthetic instrument for `trader_key` and `instrument_id` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_synthetic(
        con: &ConnectionManager,
        trader_key: &str,
        instrument_id: &InstrumentId,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<SyntheticInstrument>> {
        let key = format!("{SYNTHETICS}{REDIS_DELIMITER}{instrument_id}");
        let result = Self::read(con, trader_key, &key).await?;
        if result.is_empty() {
            return Ok(None);
        }

        let synthetic: SyntheticInstrument = Self::deserialize_payload(encoding, &result[0])?;
        Ok(Some(synthetic))
    }

    /// Loads a single account for `trader_key` and `account_id` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_account(
        con: &ConnectionManager,
        trader_key: &str,
        account_id: &AccountId,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<AccountAny>> {
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");
        let result = Self::read(con, trader_key, &key).await?;
        if result.is_empty() {
            return Ok(None);
        }

        let events: Vec<AccountState> = result
            .iter()
            .map(|bytes| Self::deserialize_payload(encoding, bytes))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let account = AccountAny::from_events(&events)?;
        Ok(Some(account))
    }

    /// Loads a single order for `trader_key` and `client_order_id` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_order(
        con: &ConnectionManager,
        trader_key: &str,
        client_order_id: &ClientOrderId,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<OrderAny>> {
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let result = Self::read(con, trader_key, &key).await?;
        if result.is_empty() {
            return Ok(None);
        }

        let events: Vec<OrderEventAny> = result
            .iter()
            .map(|bytes| Self::deserialize_payload(encoding, bytes))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let order = OrderAny::from_events(events)?;
        Ok(Some(order))
    }

    /// Loads a single position for `trader_key` and `position_id` using the specified `encoding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying read or deserialization fails.
    pub async fn load_position(
        con: &ConnectionManager,
        trader_key: &str,
        position_id: &PositionId,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<Option<Position>> {
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let result = Self::read(con, trader_key, &key).await?;
        if result.is_empty() {
            return Ok(None);
        }

        // Each update appends a full serialized position snapshot; the last entry is canonical.
        let bytes = &result[result.len() - 1];
        let position: Position = Self::deserialize_payload(encoding, bytes)?;
        Ok(Some(position))
    }

    /// Loads all `general:*` rows for `trader_key` as a map of relative keys to raw payloads.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning keys or bulk reads fail.
    pub async fn load_general_kv(
        con: &ConnectionManager,
        trader_key: &str,
    ) -> anyhow::Result<AHashMap<String, Bytes>> {
        let mut con = con.clone();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}*");
        let keys = Self::scan_keys(&mut con, pattern).await?;
        if keys.is_empty() {
            return Ok(AHashMap::new());
        }

        let values = Self::read_bulk(&con, &keys).await?;
        let prefix = format!("{trader_key}{REDIS_DELIMITER}");
        let mut out = AHashMap::with_capacity(keys.len());
        for (key, value_opt) in keys.iter().zip(values.iter()) {
            let Some(value) = value_opt else {
                continue;
            };
            let short_key = key
                .strip_prefix(&prefix)
                .unwrap_or(key.as_str())
                .to_string();
            out.insert(short_key, value.clone());
        }
        Ok(out)
    }

    /// Reads the persisted `index:order_client` hash as typed identifiers.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis `HGETALL` fails or identifier parsing is invalid.
    pub async fn load_index_order_client_hash(
        con: &ConnectionManager,
        trader_key: &str,
    ) -> anyhow::Result<AHashMap<ClientOrderId, ClientId>> {
        let mut con = con.clone();
        let full_key = format!("{trader_key}{REDIS_DELIMITER}{INDEX_ORDER_CLIENT}");
        let raw: HashMap<String, String> = con.hgetall(&full_key).await?;
        let mut out = AHashMap::with_capacity(raw.len());
        for (k, v) in raw {
            out.insert(ClientOrderId::from(k.as_str()), ClientId::new(v.as_str()));
        }
        Ok(out)
    }

    /// Resolves `index:order_position` into fully hydrated [`Position`] values.
    ///
    /// # Errors
    ///
    /// Returns an error if the index read, any position load, or deserialization fails.
    pub async fn load_index_order_position_resolved(
        con: &ConnectionManager,
        trader_key: &str,
        encoding: SerializationEncoding,
    ) -> anyhow::Result<AHashMap<ClientOrderId, Position>> {
        let mut con_h = con.clone();
        let full_key = format!("{trader_key}{REDIS_DELIMITER}{INDEX_ORDER_POSITION}");
        let raw: HashMap<String, String> = con_h.hgetall(&full_key).await?;
        let mut out = AHashMap::with_capacity(raw.len());
        let tk = Arc::<str>::from(trader_key);
        let rows: Vec<_> = stream::iter(
            raw.into_iter().map(|(coid_str, pid_str)| {
                (
                    ClientOrderId::from(coid_str.as_str()),
                    PositionId::new(pid_str),
                )
            }),
        )
        .map(|(coid, pid)| {
            let con = con.clone();
            let tk = Arc::clone(&tk);
            async move {
                match Self::load_position(&con, tk.as_ref(), &pid, encoding).await {
                    Ok(Some(position)) => Some((coid, position)),
                    Ok(None) | Err(_) => None,
                }
            }
        })
        .buffer_unordered(REDIS_KEY_LOAD_CONCURRENCY)
        .collect()
        .await;

        for pair in rows.into_iter().flatten() {
            out.insert(pair.0, pair.1);
        }
        Ok(out)
    }

    fn get_collection_key(key: &str) -> anyhow::Result<&str> {
        key.split_once(REDIS_DELIMITER)
            .map(|(collection, _)| collection)
            .ok_or_else(|| {
                anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
            })
    }

    async fn read_index(conn: &mut ConnectionManager, key: &str) -> anyhow::Result<Vec<Bytes>> {
        let index_key = get_index_key(key)?;
        match index_key {
            INDEX_ORDER_IDS => Self::read_set(conn, key).await,
            INDEX_ORDER_POSITION => Self::read_hset(conn, key).await,
            INDEX_ORDER_CLIENT => Self::read_hset(conn, key).await,
            INDEX_ORDERS => Self::read_set(conn, key).await,
            INDEX_ORDERS_OPEN => Self::read_set(conn, key).await,
            INDEX_ORDERS_CLOSED => Self::read_set(conn, key).await,
            INDEX_ORDERS_EMULATED => Self::read_set(conn, key).await,
            INDEX_ORDERS_INFLIGHT => Self::read_set(conn, key).await,
            INDEX_POSITIONS => Self::read_set(conn, key).await,
            INDEX_POSITIONS_OPEN => Self::read_set(conn, key).await,
            INDEX_POSITIONS_CLOSED => Self::read_set(conn, key).await,
            _ => anyhow::bail!("Index unknown '{index_key}' on read"),
        }
    }

    async fn read_string(conn: &mut ConnectionManager, key: &str) -> anyhow::Result<Vec<Bytes>> {
        let result: Vec<u8> = conn.get(key).await?;

        if result.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![Bytes::from(result)])
        }
    }

    async fn read_set(conn: &mut ConnectionManager, key: &str) -> anyhow::Result<Vec<Bytes>> {
        let result: Vec<Bytes> = conn.smembers(key).await?;
        Ok(result)
    }

    async fn read_hset(conn: &mut ConnectionManager, key: &str) -> anyhow::Result<Vec<Bytes>> {
        let result: HashMap<String, String> = conn.hgetall(key).await?;
        let json = serde_json::to_string(&result)?;
        Ok(vec![Bytes::from(json.into_bytes())])
    }

    async fn read_list(conn: &mut ConnectionManager, key: &str) -> anyhow::Result<Vec<Bytes>> {
        let result: Vec<Bytes> = conn.lrange(key, 0, -1).await?;
        Ok(result)
    }
}

fn is_timestamp_field(key: &str) -> bool {
    let expire_match = key == "expire_time_ns";
    let ts_match = key.starts_with("ts_");
    expire_match || ts_match
}

fn convert_timestamps(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if is_timestamp_field(key)
                    && let Value::Number(n) = v
                    && let Some(n) = n.as_u64()
                {
                    let dt = DateTime::<Utc>::from_timestamp_nanos(n as i64);
                    *v = Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
                }
                convert_timestamps(v);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                convert_timestamps(item);
            }
        }
        _ => {}
    }
}

fn convert_timestamp_strings(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if is_timestamp_field(key)
                    && let Value::String(s) = v
                    && let Ok(dt) = DateTime::parse_from_rfc3339(s)
                {
                    *v = Value::Number(
                        (dt.with_timezone(&Utc)
                            .timestamp_nanos_opt()
                            .expect("Invalid DateTime") as u64)
                            .into(),
                    );
                }
                convert_timestamp_strings(v);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                convert_timestamp_strings(item);
            }
        }
        _ => {}
    }
}
