use std::str::FromStr;

use futures;
use log::{debug, error, info, warn};
use serde::Deserialize;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcBlockConfig, RpcGetVoteAccountsConfig, RpcProgramAccountsConfig,
};
use solana_rpc_client_api::filter::{Memcmp, RpcFilterType};
use solana_rpc_client_api::request::RpcRequest;
use solana_rpc_client_api::response::{RpcPerfSample, RpcVoteAccountStatus};
use solana_sdk::account_utils::StateMut;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake;
use solana_sdk::stake::state::StakeStateV2;
use solana_sdk::sysvar;
use solana_sdk::sysvar::clock::Clock;
use solana_sdk::sysvar::stake_history;
use std::collections::HashMap;

use crate::solana::vote_accounts::LeaderSchedule;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio;

/// Protocol target slot time after SIMD-0525 (https://solana.com/upgrades/reduced-slot-times).
/// Used only when the performance samples cannot provide a measured value.
pub const DEFAULT_SLOT_TIME_MS: u64 = 200;

/// Minimum delinquency distance in slots, regardless of the configured seconds.
pub const MIN_DELINQUENT_SLOT_DISTANCE: u64 = 32;

/// Average slot time in milliseconds over a set of performance samples, or
/// `None` when the samples contain no slots.
pub fn average_slot_time_ms(samples: &[RpcPerfSample]) -> Option<u64> {
    let (slots, secs) = samples.iter().fold(
        (0u64, 0u64),
        |(slots, secs),
         RpcPerfSample {
             num_slots,
             sample_period_secs,
             ..
         }| {
            (
                slots.saturating_add(*num_slots),
                secs.saturating_add((*sample_period_secs).into()),
            )
        },
    );
    secs.saturating_mul(1000).checked_div(slots)
}

/// Average slot time from performance samples, falling back to
/// [`DEFAULT_SLOT_TIME_MS`] (with a warning) when nothing can be measured.
pub fn slot_time_ms_or_default(samples: &[RpcPerfSample]) -> u64 {
    match average_slot_time_ms(samples) {
        Some(ms) if ms > 0 => ms,
        _ => {
            warn!(
                "Performance samples contain no slots; assuming the default slot time of {} ms",
                DEFAULT_SLOT_TIME_MS
            );
            DEFAULT_SLOT_TIME_MS
        }
    }
}

/// Convert a wall-clock duration to a slot count using the measured slot time,
/// rounded to the nearest slot.
pub fn seconds_to_slots(seconds: u64, slot_time_ms: u64) -> u64 {
    let slot_time_ms = if slot_time_ms == 0 {
        DEFAULT_SLOT_TIME_MS
    } else {
        slot_time_ms
    };
    let ms = seconds.saturating_mul(1000);
    (ms + slot_time_ms / 2) / slot_time_ms
}

/// Delinquency distance in slots: `max(32, round(seconds * 1000 / slot_time_ms))`.
pub fn delinquency_slots(delinquency_seconds: u64, slot_time_ms: u64) -> u64 {
    seconds_to_slots(delinquency_seconds, slot_time_ms).max(MIN_DELINQUENT_SLOT_DISTANCE)
}

/// Milliseconds until the next leader slot after `current_slot`, or -1 when
/// there is none.
pub fn ms_to_next_slot(current_slot: u64, leader_slots: &[u64], slot_time_ms: u64) -> i64 {
    match leader_slots
        .iter()
        .filter(|slot| **slot > current_slot)
        .min()
    {
        Some(next_slot) => ((next_slot - current_slot).saturating_mul(slot_time_ms)) as i64,
        None => -1,
    }
}

/// Whether a vote account registered a BLS pubkey, read from a `getAccountInfo` (jsonParsed)
/// result. Returns `None` when the parsed vote state has no `blsPubkeyCompressed` field at all
/// (RPC node older than agave 4.3), `Some(false)` when it is null, `Some(true)` otherwise.
pub fn bls_pubkey_registered(account_info_result: &serde_json::Value) -> Option<bool> {
    let info = account_info_result
        .get("value")?
        .get("data")?
        .get("parsed")?
        .get("info")?;
    let bls = info.as_object()?.get("blsPubkeyCompressed")?;
    Some(!bls.is_null())
}

pub struct SolanaClient {
    client: RpcClient,
    vote_account: String,
    identity_account: String,
    block_rewards: HashMap<u64, i64>,
    current_epoch: Option<u64>,
}

pub struct StakeState {
    pub activated_stake: u64,
    pub activating_stake: u64,
    pub deactivating_stake: u64,
    pub locked_stake: u64,
    pub activated_stake_accounts: u64,
    pub activating_stake_accounts: u64,
    pub deactivating_stake_accounts: u64,
}

#[derive(Debug, Deserialize)]
struct KrakenResponse {
    result: ResultData,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct ResultData {
    SOLUSD: TickerData,
}

#[derive(Debug, Deserialize)]
struct TickerData {
    c: [String; 2], // Array for the closing price and volume
}

impl SolanaClient {
    pub fn new(url: &str, identity_account: &str, vote_account: &str) -> SolanaClient {
        let client = RpcClient::new_with_timeout(url.to_string(), Duration::from_secs(30));
        SolanaClient {
            client,
            vote_account: vote_account.to_string(),
            identity_account: identity_account.to_string(),
            block_rewards: HashMap::new(),
            current_epoch: None,
        }
    }

    pub async fn get_slot(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let slot = self.client.get_slot().await?;
        Ok(slot)
    }

    pub async fn get_epoch(&self) -> Result<(i64, i64), Box<dyn std::error::Error + Send + Sync>> {
        let epoch = self.client.get_epoch_info().await?;
        Ok((
            epoch.epoch as i64,
            ((epoch.slot_index as f64 / epoch.slots_in_epoch as f64) * 10000.0) as i64,
        ))
    }

    pub async fn get_stake_details(
        &self,
    ) -> Result<StakeState, Box<dyn std::error::Error + Send + Sync>> {
        let program_accounts_config = RpcProgramAccountsConfig {
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                ..RpcAccountInfoConfig::default()
            },
            filters: Some(vec![
                // Filter by `StakeStateV2::Stake(_, _)`
                RpcFilterType::Memcmp(Memcmp::new_base58_encoded(0, &[2, 0, 0, 0])),
                // Filter by `Delegation::voter_pubkey`, which begins at byte offset 124
                RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
                    124,
                    &Pubkey::from_str(&self.vote_account)?.to_bytes(),
                )),
            ]),
            ..RpcProgramAccountsConfig::default()
        };
        let mut stake_details = StakeState {
            activated_stake: 0,
            activating_stake: 0,
            deactivating_stake: 0,
            locked_stake: 0,
            activated_stake_accounts: 0,
            activating_stake_accounts: 0,
            deactivating_stake_accounts: 0,
        };
        let stake_accounts = self
            .client
            .get_program_accounts_with_config(&stake::program::id(), program_accounts_config)
            .await?;
        let clock_account = self.client.get_account(&sysvar::clock::id()).await?;
        let stake_history_account = self.client.get_account(&stake_history::id()).await?;
        let clock: Clock = solana_sdk::account::from_account(&clock_account)
            .ok_or_else(|| "Error parsing clock account")?;
        let stake_history: solana_sdk::stake_history::StakeHistory =
            solana_sdk::account::from_account(&stake_history_account)
                .ok_or_else(|| "Error parsing stake_history_account")?;
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let unix_timestamp = since_the_epoch.as_secs() as i64;
        for (stake_pubkey, stake_account) in stake_accounts {
            let stake_state: StakeStateV2 = stake_account.state()?;
            match stake_state {
                StakeStateV2::Initialized(_) => {
                    println!("pubkey: {:?}", stake_pubkey);
                }
                StakeStateV2::Stake(_, stake, _) => {
                    let account = stake.delegation.stake_activating_and_deactivating(
                        clock.epoch,
                        &stake_history,
                        Some(520), // Todo: hack but it's not relevant anymore
                    );
                    let meta = stake_state
                        .meta()
                        .ok_or_else(|| "Error parsing stake account meta info")?;
                    if meta.lockup.custodian
                        != Pubkey::from_str("11111111111111111111111111111111")?
                        && meta.lockup.unix_timestamp > unix_timestamp
                        && account.effective > 0
                    {
                        stake_details.locked_stake += account.effective;
                    }
                    stake_details.activated_stake += account.effective;
                    stake_details.activating_stake += account.activating;
                    stake_details.deactivating_stake += account.deactivating;

                    if account.effective > 0 {
                        stake_details.activated_stake_accounts += 1;
                    } else if account.activating > 0 {
                        stake_details.activating_stake_accounts += 1;
                    }
                    if account.deactivating > 0 {
                        stake_details.deactivating_stake_accounts += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(stake_details)
    }
    pub async fn get_identity_balance(
        &self,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let balance = self
            .client
            .get_balance(&Pubkey::from_str(&self.identity_account)?)
            .await?;
        Ok(balance)
    }
    pub async fn get_vote_balance(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let balance = self
            .client
            .get_balance(&Pubkey::from_str(&self.vote_account)?)
            .await?;
        Ok(balance)
    }
    /// Leader schedule of the current epoch as identity => absolute leader slots. The schedule
    /// is fixed for the epoch, so callers cache it and refetch only when the epoch changes.
    pub async fn get_leader_schedule(
        &self,
    ) -> Result<LeaderSchedule, Box<dyn std::error::Error + Send + Sync>> {
        let epoch_info = self.client.get_epoch_info().await?;
        let epoch_schedule = self.client.get_epoch_schedule().await?;
        let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch_info.epoch);
        let leader_schedule = self
            .client
            .get_leader_schedule(Some(first_slot_in_epoch))
            .await?
            .ok_or("Error parsing leader schedule")?;

        Ok(leader_schedule
            .into_iter()
            .map(|(identity, slot_indexes)| {
                let slots = slot_indexes
                    .into_iter()
                    .map(|index| index as u64 + first_slot_in_epoch)
                    .collect();
                (identity, slots)
            })
            .collect())
    }

    pub async fn get_block_production(
        &self,
    ) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
        let blocks = self.client.get_block_production().await?;
        let my_blocks = blocks
            .value
            .by_identity
            .iter()
            .filter(|(identity, _)| **identity == self.identity_account)
            .next();
        let mut bp: (usize, usize) = (0, 0);
        if let Some((_, block_production)) = my_blocks {
            bp = *block_production;
        }
        Ok(bp)
    }

    pub async fn get_block_rewards(
        &self,
        slot: u64,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        match self
            .client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    transaction_details: None,
                    rewards: Some(true),
                    max_supported_transaction_version: Some(0),
                    ..RpcBlockConfig::default()
                },
            )
            .await
        {
            Ok(block) => {
                let rewards = block.rewards.ok_or_else(|| "Error fetching rewards")?;
                if rewards.is_empty() {
                    Ok(0)
                } else {
                    Ok(rewards[0].lamports)
                }
            }
            Err(e) => {
                let error = e.to_string();
                if error.contains("skipped") {
                    Ok(0)
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    pub async fn get_block_rewards_sum(
        &mut self,
        current_slot: u64,
        current_epoch: u64,
        leader_slots: Vec<u64>,
        recent_window_slots: u64,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // New epoch detection and reset
        if let Some(cached_epoch) = self.current_epoch {
            if current_epoch != cached_epoch {
                info!("New epoch detected: {} -> {}", cached_epoch, current_epoch);
                self.block_rewards.clear();
            }
        }
        self.current_epoch = Some(current_epoch);

        // Filter slots that need fetching: not already cached, not in future, and within reasonable range
        let slots_to_fetch: Vec<u64> = leader_slots
            .iter()
            .filter(|&slot| {
                !self.block_rewards.contains_key(slot)
                    && *slot <= current_slot
                    && *slot > current_slot.saturating_sub(recent_window_slots) // Only fetch recent slots
            })
            .copied()
            .collect();

        if slots_to_fetch.is_empty() {
            let sum: i64 = self.block_rewards.values().sum();
            return Ok(sum);
        }

        // Sort by descending order (newest first) to prioritize recent slots
        let mut sorted_slots = slots_to_fetch;
        sorted_slots.sort_by(|a, b| b.cmp(a));

        // Log how far behind we are from the current slot
        if let Some(&newest_target_slot) = sorted_slots.first() {
            let slots_behind = current_slot.saturating_sub(newest_target_slot);
            info!(
                "Latest Slot: {} | Target Slot: {} is {} slots behind | Fetching {} total slots",
                current_slot,
                newest_target_slot,
                slots_behind,
                sorted_slots.len()
            );
        }

        // Process in batches to avoid overwhelming the RPC
        const BATCH_SIZE: usize = 10;
        let total_start = std::time::Instant::now();
        let mut total_fetched = 0;

        for batch in sorted_slots.chunks(BATCH_SIZE) {
            let batch_start = std::time::Instant::now();
            info!(
                "Fetching block rewards for {} slots in parallel: {:?}",
                batch.len(),
                batch
            );

            // Create futures for parallel fetching
            let futures: Vec<_> = batch
                .iter()
                .map(|&slot| self.get_block_rewards(slot))
                .collect();

            // Execute all requests in parallel
            let results = futures::future::join_all(futures).await;

            let mut batch_success_count = 0;
            // Process results
            for (i, result) in results.into_iter().enumerate() {
                let slot = batch[i];
                match result {
                    Ok(rewards) => {
                        self.block_rewards.insert(slot, rewards);
                        batch_success_count += 1;
                    }
                    Err(e) => {
                        error!("Error fetching block rewards for slot {}: {}", slot, e);
                        // Continue with other slots, don't fail the entire batch
                    }
                }
            }

            let batch_duration = batch_start.elapsed();
            info!(
                "Fetched {} out of {} slots in {:?} (avg: {:.1}ms per slot)",
                batch_success_count,
                batch.len(),
                batch_duration,
                batch_duration.as_millis() as f64 / batch.len() as f64
            );

            total_fetched += batch_success_count;

            // Add a small delay between batches to be nice to the RPC
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        let total_duration = total_start.elapsed();
        if total_fetched > 0 {
            info!(
                "Total: fetched {} slots in {:?} (avg: {:.1}ms per slot)",
                total_fetched,
                total_duration,
                total_duration.as_millis() as f64 / total_fetched as f64
            );
        }

        let sum: i64 = self.block_rewards.values().sum();
        Ok(sum)
    }

    pub async fn get_jito_tips(
        &self,
        epoch: i64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let (addr, _) = Pubkey::find_program_address(
            &[
                b"TIP_DISTRIBUTION_ACCOUNT",
                Pubkey::from_str(&self.vote_account.clone())?
                    .to_bytes()
                    .as_ref(),
                epoch.to_le_bytes().as_ref(),
            ],
            &Pubkey::from_str("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7")?,
        );
        let balance = self.client.get_balance(&addr).await?;
        Ok(balance)
    }

    pub async fn get_sol_usd_price(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Fetching sol price from kraken");
        let resp = reqwest::get("https://api.kraken.com/0/public/Ticker?pair=SOLUSD").await?;
        let data: KrakenResponse = resp.json().await?;
        debug!("Kraken response: {:?}", data);
        let price_float: f64 = data.result.SOLUSD.c[0].parse()?;
        Ok((price_float * 1e5) as i64)
    }

    /// Average slot time measured from the last 60 performance samples (~60 min),
    /// falling back to [`DEFAULT_SLOT_TIME_MS`] when the samples contain no slots.
    pub async fn get_slot_time_ms(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let samples = self.client.get_recent_performance_samples(Some(60)).await?;
        Ok(slot_time_ms_or_default(&samples))
    }

    pub async fn get_last_block_rewards(
        &self,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let mut keys: Vec<u64> = self.block_rewards.keys().cloned().collect();
        keys.sort();
        let block_rewards: Vec<_> = keys
            .iter()
            .map(|&k| (k, self.block_rewards.get(&k).unwrap()))
            .filter(|(_, reward)| **reward > 0)
            .collect();

        let last_4 = &block_rewards[block_rewards.len().saturating_sub(4)..];
        let mut sum = 0;
        for (_, rewards) in last_4 {
            sum += **rewards;
        }
        Ok(sum / 4)
    }

    /// `getAccountInfo` with `jsonParsed` on the vote account to check whether a BLS pubkey is
    /// registered (required for admission to the Alpenglow validator set). `Ok(None)` means the
    /// RPC node does not report the field. Meant to be polled once per epoch.
    pub async fn get_bls_pubkey_registered(
        &self,
    ) -> Result<Option<bool>, Box<dyn std::error::Error + Send + Sync>> {
        let result: serde_json::Value = self
            .client
            .send(
                RpcRequest::GetAccountInfo,
                serde_json::json!([
                    self.vote_account,
                    RpcAccountInfoConfig {
                        encoding: Some(solana_account_decoder::UiAccountEncoding::JsonParsed),
                        ..RpcAccountInfoConfig::default()
                    }
                ]),
            )
            .await?;
        if result.get("value").map_or(true, |v| v.is_null()) {
            return Err(format!("vote account {} not found", self.vote_account).into());
        }
        Ok(bls_pubkey_registered(&result))
    }

    /// One `getVoteAccounts` call per loop iteration, keeping unstaked delinquents so that
    /// voting status, vote/root distances, credits and cluster statistics all come from the
    /// same response. `delinquent_slot_distance` is derived from `delinquency_seconds`.
    pub async fn get_vote_accounts(
        &self,
        delinquent_slot_distance: u64,
    ) -> Result<RpcVoteAccountStatus, Box<dyn std::error::Error + Send + Sync>> {
        let vote_accounts = self
            .client
            .get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
                keep_unstaked_delinquents: Some(true),
                delinquent_slot_distance: Some(delinquent_slot_distance),
                ..RpcGetVoteAccountsConfig::default()
            })
            .await?;
        Ok(vote_accounts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(num_slots: u64, sample_period_secs: u16) -> RpcPerfSample {
        RpcPerfSample {
            slot: 0,
            num_transactions: 0,
            num_non_vote_transactions: None,
            num_slots,
            sample_period_secs,
        }
    }

    #[test]
    fn slot_time_is_measured_from_samples() {
        // 300 slots in 60 s => 200 ms
        let samples = vec![sample(150, 30), sample(150, 30)];
        assert_eq!(average_slot_time_ms(&samples), Some(200));
        assert_eq!(slot_time_ms_or_default(&samples), 200);
        // 150 slots in 60 s => 400 ms
        let samples = vec![sample(150, 60)];
        assert_eq!(slot_time_ms_or_default(&samples), 400);
    }

    #[test]
    fn slot_time_falls_back_to_default_when_samples_have_no_slots() {
        let samples = vec![sample(0, 60), sample(0, 60)];
        assert_eq!(average_slot_time_ms(&samples), None);
        assert_eq!(slot_time_ms_or_default(&samples), DEFAULT_SLOT_TIME_MS);
        assert_eq!(slot_time_ms_or_default(&[]), DEFAULT_SLOT_TIME_MS);
        assert_eq!(DEFAULT_SLOT_TIME_MS, 200);
    }

    #[test]
    fn delinquency_seconds_convert_to_slots() {
        assert_eq!(delinquency_slots(60, 200), 300);
        assert_eq!(delinquency_slots(60, 400), 150);
        // Floor of 32 slots
        assert_eq!(delinquency_slots(1, 400), 32);
        assert_eq!(delinquency_slots(0, 200), 32);
        // Rounds to nearest slot
        assert_eq!(seconds_to_slots(1, 300), 3);
        assert_eq!(seconds_to_slots(600, 200), 3000);
        // A zero slot time never divides by zero
        assert_eq!(seconds_to_slots(60, 0), 300);
    }

    #[test]
    fn bls_pubkey_is_read_from_parsed_vote_state() {
        let registered = serde_json::json!({
            "context": {"slot": 1},
            "value": {"data": {"parsed": {"info": {
                "nodePubkey": "x",
                "blsPubkeyCompressed": "8f3ab2"
            }, "type": "vote"}, "program": "vote"}}
        });
        assert_eq!(bls_pubkey_registered(&registered), Some(true));

        let unregistered = serde_json::json!({
            "value": {"data": {"parsed": {"info": {"blsPubkeyCompressed": null}}}}
        });
        assert_eq!(bls_pubkey_registered(&unregistered), Some(false));

        let old_rpc = serde_json::json!({
            "value": {"data": {"parsed": {"info": {"nodePubkey": "x"}}}}
        });
        assert_eq!(bls_pubkey_registered(&old_rpc), None);

        // Base64-encoded (unparsed) data never panics.
        let unparsed = serde_json::json!({"value": {"data": ["AAAA", "base64"]}});
        assert_eq!(bls_pubkey_registered(&unparsed), None);
    }

    #[test]
    fn ms_to_next_slot_uses_slot_time() {
        assert_eq!(ms_to_next_slot(100, &[90, 110, 120], 200), 2000);
        assert_eq!(ms_to_next_slot(100, &[90, 110, 120], 400), 4000);
        assert_eq!(ms_to_next_slot(100, &[], 200), -1);
        assert_eq!(ms_to_next_slot(130, &[90, 110, 120], 200), -1);
    }
}
