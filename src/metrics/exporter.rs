use crate::solana;
use crate::solana::validator::{self, StakeState};
use crate::solana::vote_accounts::{
    self, LeaderSchedule, RateReference, RewardRate, RewardRateWindow, VoteAccountAnalysis,
};
use axum::body::Body;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::{error, warn};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;

type MetricLabels = Vec<(String, String)>;
type FloatGauge = Gauge<f64, AtomicU64>;

/// Per-network collection settings, all expressed in wall-clock seconds and
/// converted to slots at runtime with the measured slot time.
#[derive(Debug, Clone, Copy)]
pub struct CollectorSettings {
    /// A validator whose last vote is older than this is reported as delinquent.
    pub delinquency_seconds: u64,
    /// Sliding window for `solana_epoch_credits_rate_ratio`.
    pub reward_window_seconds: u64,
    /// Lookback window for fetching block rewards of recent leader slots.
    pub recent_window_seconds: u64,
}

pub struct Metrics {
    rpc_url: String,
    identity_account: String,
    vote_account: String,
    static_labels: Arc<BTreeMap<String, String>>,
    settings: CollectorSettings,
    pub slot: Family<MetricLabels, Gauge>,
    pub epoch: Family<MetricLabels, Gauge>,
    pub epoch_progress: Family<MetricLabels, Gauge>,
    pub stake: Family<MetricLabels, Gauge>,
    pub identity_balance: Family<MetricLabels, Gauge>,
    pub vote_account_balance: Family<MetricLabels, Gauge>,
    pub blocks: Family<MetricLabels, Gauge>,
    pub jito_tips: Family<MetricLabels, Gauge>,
    pub vote_credit_rank: Family<MetricLabels, Gauge>,
    pub usd_price: Family<MetricLabels, Gauge>,
    pub epoch_block_rewards: Family<MetricLabels, Gauge>,
    pub ms_to_next_slot: Family<MetricLabels, Gauge>,
    pub last_block_rewards: Family<MetricLabels, Gauge>,
    pub voting_status: Family<MetricLabels, Gauge>,
    pub slot_time_ms: Family<MetricLabels, Gauge>,
    pub vote_distance_slots: Family<MetricLabels, Gauge>,
    pub root_distance_slots: Family<MetricLabels, Gauge>,
    pub epoch_credits: Family<MetricLabels, Gauge>,
    pub epoch_credits_per_stake_ratio: Family<MetricLabels, FloatGauge>,
    pub epoch_credits_percentile: Family<MetricLabels, FloatGauge>,
    pub epoch_credits_rate_ratio: Family<MetricLabels, FloatGauge>,
    pub epoch_credits_rate_percentile: Family<MetricLabels, FloatGauge>,
    pub leader_slots_in_window: Family<MetricLabels, Gauge>,
    pub rate_reference_accounts: Family<MetricLabels, Gauge>,
    pub vote_account_epoch: Family<MetricLabels, Gauge>,
    pub bls_pubkey_registered: Family<MetricLabels, Gauge>,
}

impl Metrics {
    pub fn new(
        rpc_url: String,
        identity_account: String,
        vote_account: String,
        labels: BTreeMap<String, String>,
        settings: CollectorSettings,
    ) -> Metrics {
        Metrics {
            rpc_url,
            identity_account,
            vote_account,
            static_labels: Arc::new(labels),
            settings,
            slot: Family::default(),
            epoch: Family::default(),
            epoch_progress: Family::default(),
            stake: Family::default(),
            identity_balance: Family::default(),
            vote_account_balance: Family::default(),
            blocks: Family::default(),
            jito_tips: Family::default(),
            vote_credit_rank: Family::default(),
            usd_price: Family::default(),
            epoch_block_rewards: Family::default(),
            ms_to_next_slot: Family::default(),
            last_block_rewards: Family::default(),
            voting_status: Family::default(),
            slot_time_ms: Family::default(),
            vote_distance_slots: Family::default(),
            root_distance_slots: Family::default(),
            epoch_credits: Family::default(),
            epoch_credits_per_stake_ratio: Family::default(),
            epoch_credits_percentile: Family::default(),
            epoch_credits_rate_ratio: Family::default(),
            epoch_credits_rate_percentile: Family::default(),
            leader_slots_in_window: Family::default(),
            rate_reference_accounts: Family::default(),
            vote_account_epoch: Family::default(),
            bls_pubkey_registered: Family::default(),
        }
    }

    pub async fn init_registry(&self, shared_state: Arc<Mutex<AppState>>) {
        let mut state = shared_state.lock().await;

        state
            .registry
            .register("solana_slot", "Slot of cluster", self.slot.clone());

        state
            .registry
            .register("solana_epoch", "Current epoch", self.epoch.clone());

        state.registry.register(
            "solana_epoch_progress",
            "Epoch progress",
            self.epoch_progress.clone(),
        );

        state
            .registry
            .register("solana_stake", "Stake info", self.stake.clone());

        state.registry.register(
            "solana_identity_balance",
            "Identity balance",
            self.identity_balance.clone(),
        );

        state.registry.register(
            "solana_vote_account_balance",
            "Vote account balance",
            self.vote_account_balance.clone(),
        );

        state.registry.register(
            "solana_blocks",
            "Block production. 0 scheduled slots with non-zero activated stake means the validator was not admitted to the epoch's leader schedule (VAT or missing BLS pubkey)",
            self.blocks.clone(),
        );

        state
            .registry
            .register("solana_jito_tips", "Jito tips", self.jito_tips.clone());

        state.registry.register(
            "solana_vote_credit_rank",
            "DEPRECATED since Alpenglow: epoch credits are reward lamports proportional to stake, so this ranks by stake. Use solana_epoch_credits_rate_ratio / solana_epoch_credits_percentile instead. Will be removed in the next release",
            self.vote_credit_rank.clone(),
        );

        state
            .registry
            .register("solana_usd_price", "USD Price", self.usd_price.clone());

        state.registry.register(
            "solana_epoch_block_rewards",
            "Sum of block rewards this epoch",
            self.epoch_block_rewards.clone(),
        );

        state.registry.register(
            "solana_ms_to_next_slot",
            "Time to next leader slot",
            self.ms_to_next_slot.clone(),
        );

        state.registry.register(
            "solana_last_block_rewards",
            "Average of last non-zero block rewards",
            self.last_block_rewards.clone(),
        );

        state.registry.register(
            "solana_voting_status",
            "Validator voting status (-1=not found from the RPC response, 0=delinquent, 1=voting normally)",
            self.voting_status.clone(),
        );

        state.registry.register(
            "solana_slot_time_ms",
            "Average slot time in milliseconds measured from recent performance samples; used for every slot/time conversion in this exporter",
            self.slot_time_ms.clone(),
        );

        state.registry.register(
            "solana_vote_distance_slots",
            "Current slot minus the vote account's lastVote, from one getVoteAccounts/getSlot pair on the same RPC node",
            self.vote_distance_slots.clone(),
        );

        state.registry.register(
            "solana_root_distance_slots",
            "Current slot minus the vote account's rootSlot, from one getVoteAccounts/getSlot pair on the same RPC node",
            self.root_distance_slots.clone(),
        );

        state.registry.register(
            "solana_epoch_credits",
            "Credits earned in the current epoch (reward lamports since Alpenglow); 0 if the account has no entry for the current epoch",
            self.epoch_credits.clone(),
        );

        state.registry.register(
            "solana_epoch_credits_per_stake_ratio",
            "Epoch-to-date credits per activated stake divided by the cluster median over staked accounts with a current-epoch entry (1.0 = median)",
            self.epoch_credits_per_stake_ratio.clone(),
        );

        state.registry.register(
            "solana_epoch_credits_percentile",
            "Percentage (0-100) of staked vote accounts whose epoch-to-date credits per stake is strictly below ours; accounts without a current-epoch entry count as 0",
            self.epoch_credits_percentile.clone(),
        );

        state.registry.register(
            "solana_epoch_credits_rate_ratio",
            "Credits per stake earned over the last reward_window_seconds divided by the median of the same per-account quantity over accounts with no leader slot inside the window (1.0 = median; p10 of all accounts when fewer than 50 such accounts exist); our own leader slots inflate it for one window; absent until the window holds two snapshots at least 60 s apart",
            self.epoch_credits_rate_ratio.clone(),
        );

        state.registry.register(
            "solana_epoch_credits_rate_percentile",
            "Percentage (0-100) of staked vote accounts whose credits per stake earned over the last reward_window_seconds is strictly below ours, among accounts that earned anything in the window; informational only: healthy validators cluster at the same rate, so values between ~10 and ~60 are noise",
            self.epoch_credits_rate_percentile.clone(),
        );

        state.registry.register(
            "solana_leader_slots_in_window",
            "Number of our scheduled leader slots inside the reward_window_seconds window; leader rewards make solana_epoch_credits_rate_ratio spike above 1 when this is non-zero",
            self.leader_slots_in_window.clone(),
        );

        state.registry.register(
            "solana_rate_reference_accounts",
            "Number of staked vote accounts that earned credits in the reward window and had no leader slot inside it: the reference population of solana_epoch_credits_rate_ratio; below 50 the ratio falls back to the 10th percentile of all accounts",
            self.rate_reference_accounts.clone(),
        );

        state.registry.register(
            "solana_vote_account_epoch",
            "Epoch of the newest usable epochCredits entry; lags solana_epoch when the vote account or the RPC node is stale",
            self.vote_account_epoch.clone(),
        );

        state.registry.register(
            "solana_bls_pubkey_registered",
            "1 if the vote account state contains a BLS pubkey (required for admission to the Alpenglow validator set), else 0; polled once per epoch",
            self.bls_pubkey_registered.clone(),
        );
    }

    pub fn run_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = solana::validator::SolanaClient::new(
                &self.rpc_url,
                &self.identity_account,
                &self.vote_account,
            );

            // Create a channel for communicating current slot, epoch, leader slots and the
            // recent-slot window to the background task
            let (slot_tx, mut slot_rx) =
                tokio::sync::mpsc::unbounded_channel::<(u64, u64, Vec<u64>, u64)>();

            // Spawn background task for block rewards fetching
            let mut bg_client = solana::validator::SolanaClient::new(
                &self.rpc_url,
                &self.identity_account,
                &self.vote_account,
            );
            let bg_self = self.clone();
            tokio::spawn(async move {
                while let Some((current_slot, current_epoch, leader_slots, recent_window_slots)) =
                    slot_rx.recv().await
                {
                    if !leader_slots.is_empty() {
                        match bg_client
                            .get_block_rewards_sum(
                                current_slot,
                                current_epoch,
                                leader_slots,
                                recent_window_slots,
                            )
                            .await
                        {
                            Ok(block_rewards) => {
                                bg_self.set_epoch_block_rewards(block_rewards);

                                match bg_client.get_last_block_rewards().await {
                                    Ok(last_rewards) => {
                                        bg_self.set_last_block_rewards(last_rewards);
                                    }
                                    Err(e) => {
                                        error!("Error fetching last block rewards: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error fetching block rewards: {}", e);
                            }
                        }
                    }
                }
            });

            let mut reward_window = RewardRateWindow::new(self.settings.reward_window_seconds);
            // The leader schedule is fixed for an epoch: fetched once per epoch, it serves the
            // reward window's reference population and our own leader slots.
            let mut leader_schedule_cache: Option<(u64, LeaderSchedule)> = None;
            let mut rate_fallback_logged_epoch: Option<u64> = None;
            // BLS registration is checked once per epoch; a missing field is logged once.
            let mut bls_checked_epoch: Option<u64> = None;
            let mut bls_field_missing_logged = false;

            loop {
                let epoch_info = match client.get_epoch().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching epoch: {}", e);
                        None
                    }
                };

                // Measured slot time drives every slot/time conversion below.
                let slot_time_ms = match client.get_slot_time_ms().await {
                    Ok(ms) => ms,
                    Err(e) => {
                        error!(
                            "Error fetching performance samples: {}; assuming {} ms slots",
                            e,
                            validator::DEFAULT_SLOT_TIME_MS
                        );
                        validator::DEFAULT_SLOT_TIME_MS
                    }
                };
                self.set_slot_time_ms(slot_time_ms);
                let recent_window_slots =
                    validator::seconds_to_slots(self.settings.recent_window_seconds, slot_time_ms);
                let delinquent_slot_distance =
                    validator::delinquency_slots(self.settings.delinquency_seconds, slot_time_ms);

                if let Some((epoch, _)) = epoch_info {
                    let cached_epoch = leader_schedule_cache.as_ref().map(|(epoch, _)| *epoch);
                    if cached_epoch != Some(epoch as u64) {
                        match client.get_leader_schedule().await {
                            Ok(schedule) => leader_schedule_cache = Some((epoch as u64, schedule)),
                            Err(e) => {
                                error!("Error fetching leader schedule: {}", e);
                                leader_schedule_cache = None;
                            }
                        }
                    }
                }
                let empty_schedule = LeaderSchedule::new();
                let leader_schedule: &LeaderSchedule = leader_schedule_cache
                    .as_ref()
                    .map(|(_, schedule)| schedule)
                    .unwrap_or(&empty_schedule);

                // Fetch vote accounts before the reference slot. At 200 ms per slot the bank can
                // advance during getVoteAccounts, so a slot fetched first can be older than the
                // response's lastVote.
                let vote_accounts = if epoch_info.is_some() {
                    match client.get_vote_accounts(delinquent_slot_distance).await {
                        Ok(status) => Some(status),
                        Err(e) => {
                            error!("Error fetching vote accounts: {}", e);
                            None
                        }
                    }
                } else {
                    None
                };

                let slot = match client.get_slot().await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("Error fetching slot: {}", e);
                        None
                    }
                };

                if let Some(slot) = slot {
                    self.set_slot(slot);
                }

                // Everything vote-related derives from the single snapshot fetched above.
                let mut reward_rate: Option<RewardRate> = None;
                if let (Some(status), Some(current_slot), Some((epoch, _))) =
                    (vote_accounts, slot, epoch_info)
                {
                    let analysis = vote_accounts::analyze(
                        &status,
                        &self.vote_account,
                        epoch as u64,
                        current_slot,
                    );
                    self.set_vote_account_analysis(&analysis);
                    reward_rate = reward_window.push(
                        unix_time_secs(),
                        current_slot,
                        epoch as u64,
                        &status,
                        &self.vote_account,
                        leader_schedule,
                    );
                    self.set_reward_rate(reward_rate.as_ref());
                    if let Some(rate) = &reward_rate {
                        if rate.reference == RateReference::AllAccountsP10
                            && rate_fallback_logged_epoch != Some(epoch as u64)
                        {
                            warn!(
                                "Only {} accounts had no leader slot inside the reward window (minimum {}); solana_epoch_credits_rate_ratio uses the 10th percentile of all accounts for epoch {}",
                                rate.reference_accounts,
                                vote_accounts::MIN_REFERENCE_ACCOUNTS,
                                epoch
                            );
                            rate_fallback_logged_epoch = Some(epoch as u64);
                        }
                    }
                }

                if let Some(epoch_info) = epoch_info {
                    let (epoch, epoch_progress) = epoch_info;
                    self.set_epoch(epoch);
                    self.set_epoch_progress(epoch_progress);

                    if bls_checked_epoch != Some(epoch as u64) {
                        match client.get_bls_pubkey_registered().await {
                            Ok(registered) => {
                                if registered.is_none() && !bls_field_missing_logged {
                                    warn!(
                                        "RPC node does not report blsPubkeyCompressed for {}; exporting solana_bls_pubkey_registered=0",
                                        self.vote_account
                                    );
                                    bls_field_missing_logged = true;
                                }
                                self.set_bls_pubkey_registered(registered.unwrap_or(false));
                                bls_checked_epoch = Some(epoch as u64);
                            }
                            Err(e) => {
                                error!("Error fetching vote account state for BLS pubkey: {}", e);
                            }
                        }
                    }

                    let jito_tips = match client.get_jito_tips(epoch).await {
                        Ok(tips) => Some(tips),
                        Err(e) => {
                            error!("Error fetching jito tips: {}", e);
                            None
                        }
                    };

                    if let Some(jito_tips) = jito_tips {
                        self.set_jito_tips(jito_tips);
                    }

                    // Send slot/epoch info to background task
                    let leader_slots: Vec<u64> = leader_schedule
                        .get(&self.identity_account)
                        .cloned()
                        .unwrap_or_default();

                    let _ = slot_tx.send((
                        slot.unwrap_or(0),
                        epoch as u64,
                        leader_slots.clone(),
                        recent_window_slots,
                    ));

                    // Update solana_blocks{block_type="scheduled"}
                    self.set_block_scheduled(leader_slots.len() as u64);
                    self.set_leader_slots_in_window(
                        reward_rate
                            .as_ref()
                            .map(|rate| rate.leader_slots_in_window(&leader_slots)),
                    );

                    self.set_ms_to_next_slot(validator::ms_to_next_slot(
                        slot.unwrap_or(0),
                        &leader_slots,
                        slot_time_ms,
                    ));
                }

                let stake_details = match client.get_stake_details().await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("Error fetching stake details: {}", e);
                        None
                    }
                };

                if let Some(stake_details) = stake_details {
                    self.set_stake(stake_details);
                }

                let identity_balance = match client.get_identity_balance().await {
                    Ok(b) => Some(b),
                    Err(e) => {
                        error!("Error fetching identity balance: {}", e);
                        None
                    }
                };

                if let Some(identity_balance) = identity_balance {
                    self.set_identity_balance(identity_balance);
                }

                let vote_account_balance = match client.get_vote_balance().await {
                    Ok(b) => Some(b),
                    Err(e) => {
                        error!("Error fetching vote account balance: {}", e);
                        None
                    }
                };

                if let Some(vote_account_balance) = vote_account_balance {
                    self.set_vote_account_balance(vote_account_balance);
                }

                let block_production = match client.get_block_production().await {
                    Ok(b) => Some(b),
                    Err(e) => {
                        error!("Error fetching block production: {}", e);
                        None
                    }
                };

                if let Some(block_production) = block_production {
                    let (blocks_produced, blocks_total) = block_production;
                    let blocks_skipped = blocks_total - blocks_produced;
                    self.set_block_production(
                        blocks_total as u64,
                        blocks_produced as u64,
                        blocks_skipped as u64,
                    );
                }

                let usd_price = match client.get_sol_usd_price().await {
                    Ok(p) => Some(p),
                    Err(e) => {
                        error!("Error fetching USD price: {}", e);
                        None
                    }
                };

                if let Some(usd_price) = usd_price {
                    self.set_usd_price(usd_price);
                }

                // Sleep between metric updates
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            }
        })
    }

    pub fn set_slot(&self, slot: u64) {
        let labels = self.base_label_pairs();
        self.slot.get_or_create(&labels).set(slot as i64);
    }

    pub fn set_epoch(&self, epoch: i64) {
        let labels = self.base_label_pairs();
        self.epoch.get_or_create(&labels).set(epoch);
    }

    pub fn set_epoch_progress(&self, progress: i64) {
        let labels = self.base_label_pairs();
        self.epoch_progress.get_or_create(&labels).set(progress);
    }

    pub fn set_stake(&self, stake_state: StakeState) {
        self.set_stake_metric("activated", stake_state.activated_stake);
        self.set_stake_metric("activating", stake_state.activating_stake);
        self.set_stake_metric("deactivating", stake_state.deactivating_stake);
        self.set_stake_metric("locked", stake_state.locked_stake);
        self.set_stake_metric("activated_accounts", stake_state.activated_stake_accounts);
        self.set_stake_metric("activating_accounts", stake_state.activating_stake_accounts);
        self.set_stake_metric(
            "deactivating_accounts",
            stake_state.deactivating_stake_accounts,
        );
    }

    pub fn set_identity_balance(&self, balance: u64) {
        let labels = self.base_label_pairs();
        self.identity_balance
            .get_or_create(&labels)
            .set(balance as i64);
    }

    pub fn set_vote_account_balance(&self, balance: u64) {
        let labels = self.base_label_pairs();
        self.vote_account_balance
            .get_or_create(&labels)
            .set(balance as i64);
    }

    pub fn set_block_scheduled(&self, scheduled: u64) {
        self.set_block_metric("scheduled", scheduled);
    }

    pub fn set_block_production(&self, total: u64, produced: u64, skipped: u64) {
        self.set_block_metric("total", total);
        self.set_block_metric("produced", produced);
        self.set_block_metric("skipped", skipped);
    }

    pub fn set_jito_tips(&self, tips: u64) {
        let labels = self.base_label_pairs();
        self.jito_tips.get_or_create(&labels).set(tips as i64);
    }

    pub fn set_vote_credit_rank(&self, rank: u32) {
        let labels = self.base_label_pairs();
        self.vote_credit_rank
            .get_or_create(&labels)
            .set(rank as i64);
    }

    pub fn set_usd_price(&self, price: i64) {
        let labels = self.base_label_pairs();
        self.usd_price.get_or_create(&labels).set(price);
    }

    pub fn set_epoch_block_rewards(&self, block_rewards: i64) {
        let labels = self.base_label_pairs();
        self.epoch_block_rewards
            .get_or_create(&labels)
            .set(block_rewards);
    }

    pub fn set_ms_to_next_slot(&self, ms_to_next_slot: i64) {
        let labels = self.base_label_pairs();
        self.ms_to_next_slot
            .get_or_create(&labels)
            .set(ms_to_next_slot);
    }

    pub fn set_last_block_rewards(&self, last_block_rewards: i64) {
        let labels = self.base_label_pairs();
        self.last_block_rewards
            .get_or_create(&labels)
            .set(last_block_rewards);
    }

    pub fn set_voting_status(&self, status: i64) {
        let labels = self.base_label_pairs();
        self.voting_status.get_or_create(&labels).set(status);
    }

    pub fn set_vote_account_analysis(&self, analysis: &VoteAccountAnalysis) {
        self.set_voting_status(analysis.voting_status);
        self.set_vote_credit_rank(analysis.vote_credit_rank);
        self.set_epoch_credits(analysis.epoch_credits);
        self.set_or_remove_i64(&self.vote_distance_slots, analysis.vote_distance_slots);
        self.set_or_remove_i64(&self.root_distance_slots, analysis.root_distance_slots);
        self.set_or_remove_i64(
            &self.vote_account_epoch,
            analysis.vote_account_epoch.map(|epoch| epoch as i64),
        );
        self.set_or_remove_f64(
            &self.epoch_credits_per_stake_ratio,
            analysis.credits_per_stake_ratio,
        );
        self.set_or_remove_f64(&self.epoch_credits_percentile, analysis.credits_percentile);
    }

    /// Export the windowed reward rate, or drop both samples while the window is not ready.
    pub fn set_reward_rate(&self, rate: Option<&RewardRate>) {
        self.set_or_remove_f64(&self.epoch_credits_rate_ratio, rate.map(|r| r.ratio));
        self.set_or_remove_f64(
            &self.epoch_credits_rate_percentile,
            rate.map(|r| r.percentile),
        );
        self.set_or_remove_i64(
            &self.rate_reference_accounts,
            rate.map(|r| r.reference_accounts.min(i64::MAX as u64) as i64),
        );
    }

    /// Export the number of our leader slots inside the reward window, or drop the sample
    /// while the window is not ready.
    pub fn set_leader_slots_in_window(&self, count: Option<u64>) {
        self.set_or_remove_i64(
            &self.leader_slots_in_window,
            count.map(|c| c.min(i64::MAX as u64) as i64),
        );
    }

    pub fn set_epoch_credits(&self, credits: u64) {
        let labels = self.base_label_pairs();
        self.epoch_credits
            .get_or_create(&labels)
            .set(credits.min(i64::MAX as u64) as i64);
    }

    /// Set the gauge, or drop its sample when there is no value to report.
    fn set_or_remove_i64(&self, family: &Family<MetricLabels, Gauge>, value: Option<i64>) {
        let labels = self.base_label_pairs();
        match value {
            Some(v) => {
                family.get_or_create(&labels).set(v);
            }
            None => {
                family.remove(&labels);
            }
        }
    }

    /// Set the gauge, or drop its sample when there is no value to report (NaN/omitted).
    fn set_or_remove_f64(&self, family: &Family<MetricLabels, FloatGauge>, value: Option<f64>) {
        let labels = self.base_label_pairs();
        match value {
            Some(v) if v.is_finite() => {
                family.get_or_create(&labels).set(v);
            }
            _ => {
                family.remove(&labels);
            }
        }
    }

    pub fn set_bls_pubkey_registered(&self, registered: bool) {
        let labels = self.base_label_pairs();
        self.bls_pubkey_registered
            .get_or_create(&labels)
            .set(registered as i64);
    }

    pub fn set_slot_time_ms(&self, slot_time_ms: u64) {
        let labels = self.base_label_pairs();
        self.slot_time_ms
            .get_or_create(&labels)
            .set(slot_time_ms as i64);
    }

    fn set_stake_metric(&self, stake_type: &str, value: u64) {
        let labels = self.labels_with("stake_type", stake_type);
        self.stake.get_or_create(&labels).set(value as i64);
    }

    fn set_block_metric(&self, block_type: &str, value: u64) {
        let labels = self.labels_with("block_type", block_type);
        self.blocks.get_or_create(&labels).set(value as i64);
    }

    fn base_label_pairs(&self) -> MetricLabels {
        self.static_labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn labels_with(&self, key: &str, value: impl Into<String>) -> MetricLabels {
        let mut labels = self.base_label_pairs();
        labels.push((key.to_string(), value.into()));
        labels
    }
}

fn unix_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct AppState {
    pub registry: Registry,
}

pub async fn metrics_handler(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().await;
    let mut body = String::new();
    if let Err(e) = encode(&mut body, &state.registry) {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Error encoding metrics: {}", e)))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/openmetrics-text")
        .body(Body::from(body))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_metrics() -> Metrics {
        let mut labels = BTreeMap::new();
        labels.insert("network".to_string(), "solana_mainnet".to_string());
        labels.insert(
            "vote_account".to_string(),
            "Ours11111111111111111111111111111111111111111".to_string(),
        );
        labels.insert("stage".to_string(), "mainnet".to_string());
        Metrics::new(
            "http://localhost:8899".to_string(),
            "OursIdentity11111111111111111111111111111111".to_string(),
            "Ours11111111111111111111111111111111111111111".to_string(),
            labels,
            CollectorSettings {
                delinquency_seconds: 60,
                reward_window_seconds: 900,
                recent_window_seconds: 600,
            },
        )
    }

    /// Sample lines inside one metric family come out of a HashMap, so sort them per block
    /// to make the comparison deterministic.
    fn normalize(body: &str) -> String {
        let mut out = Vec::new();
        let mut block: Vec<&str> = Vec::new();
        for line in body.lines() {
            if line.starts_with('#') {
                block.sort_unstable();
                out.append(&mut block);
                out.push(line);
            } else {
                block.push(line);
            }
        }
        block.sort_unstable();
        out.append(&mut block);
        out.join("\n") + "\n"
    }

    async fn render(metrics: &Metrics) -> String {
        let state = Arc::new(Mutex::new(AppState {
            registry: Registry::default(),
        }));
        metrics.init_registry(state.clone()).await;
        let mut body = String::new();
        encode(&mut body, &state.lock().await.registry).unwrap();
        body
    }

    /// Snapshot of the full `/metrics` output: metric names, types, HELP text and label sets
    /// must not change without updating `fixtures/metrics_snapshot.txt` deliberately.
    #[tokio::test]
    async fn metrics_output_matches_snapshot() {
        let metrics = fixture_metrics();
        metrics.set_slot(462_000_500);
        metrics.set_epoch(1070);
        metrics.set_epoch_progress(1234);
        metrics.set_stake(StakeState {
            activated_stake: 100_000,
            activating_stake: 10,
            deactivating_stake: 20,
            locked_stake: 30,
            activated_stake_accounts: 4,
            activating_stake_accounts: 1,
            deactivating_stake_accounts: 2,
        });
        metrics.set_identity_balance(5_000_000_000);
        metrics.set_vote_account_balance(2_000_000_000);
        metrics.set_block_scheduled(12);
        metrics.set_block_production(10, 9, 1);
        metrics.set_jito_tips(777);
        metrics.set_usd_price(15_012_345);
        metrics.set_epoch_block_rewards(123_456);
        metrics.set_ms_to_next_slot(4_000);
        metrics.set_last_block_rewards(11_111);
        metrics.set_slot_time_ms(200);
        metrics.set_bls_pubkey_registered(true);
        metrics.set_vote_account_analysis(&VoteAccountAnalysis {
            voting_status: 1,
            vote_distance_slots: Some(3),
            root_distance_slots: Some(35),
            epoch_credits: 1_000,
            vote_account_epoch: Some(1070),
            activated_stake: 100_000,
            credits_per_stake: Some(0.01),
            cluster_median_credits_per_stake: Some(0.0105),
            credits_per_stake_ratio: Some(0.95),
            credits_percentile: Some(40.0),
            vote_credit_rank: 2,
        });
        let rate = RewardRate {
            ratio: 1.25,
            percentile: 60.0,
            reference: RateReference::ZeroLeaderMedian,
            reference_accounts: 512,
            first_slot: 462_000_000,
            last_slot: 462_000_500,
        };
        metrics.set_reward_rate(Some(&rate));
        metrics.set_leader_slots_in_window(Some(rate.leader_slots_in_window(&[
            461_999_999,
            462_000_000,
            462_000_004,
            462_000_500,
        ])));

        let body = normalize(&render(&metrics).await);
        let expected = normalize(include_str!("fixtures/metrics_snapshot.txt"));
        assert_eq!(body, expected, "\n--- rendered /metrics ---\n{body}");
    }

    /// Metrics without a value are omitted instead of exporting a fake number.
    #[tokio::test]
    async fn optional_metrics_are_removed_when_unavailable() {
        let metrics = fixture_metrics();
        metrics.set_reward_rate(Some(&RewardRate {
            ratio: 1.25,
            percentile: 60.0,
            reference: RateReference::AllAccountsP10,
            reference_accounts: 12,
            first_slot: 1,
            last_slot: 2,
        }));
        metrics.set_leader_slots_in_window(Some(2));
        metrics.set_vote_account_analysis(&VoteAccountAnalysis {
            voting_status: -1,
            vote_distance_slots: None,
            root_distance_slots: None,
            epoch_credits: 0,
            vote_account_epoch: None,
            activated_stake: 0,
            credits_per_stake: None,
            cluster_median_credits_per_stake: None,
            credits_per_stake_ratio: None,
            credits_percentile: None,
            vote_credit_rank: 0,
        });
        metrics.set_reward_rate(None);
        metrics.set_leader_slots_in_window(None);

        let body = render(&metrics).await;
        assert!(body.contains("solana_voting_status{"), "{body}");
        assert!(body.contains("} -1\n"), "{body}");
        for absent in [
            "solana_vote_distance_slots{",
            "solana_root_distance_slots{",
            "solana_vote_account_epoch{",
            "solana_epoch_credits_per_stake_ratio{",
            "solana_epoch_credits_percentile{",
            "solana_epoch_credits_rate_ratio{",
            "solana_epoch_credits_rate_percentile{",
            "solana_leader_slots_in_window{",
            "solana_rate_reference_accounts{",
        ] {
            assert!(!body.contains(absent), "{absent} should be absent:\n{body}");
        }
    }
}
