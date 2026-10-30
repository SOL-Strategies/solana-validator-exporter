# Solana Validator Exporter

This is a Prometheus exporter for Solana validators that supports monitoring multiple validators across different networks (mainnet, testnet, devnet).

## Features

- **Multi-Network Support**: Monitor validators on mainnet, testnet, and devnet simultaneously
- **Multi-Validator Support**: Monitor multiple validators per network
- **Comprehensive Metrics**: Track validator performance, financial metrics, and network statistics
- **Network Labels**: All metrics include network and vote account labels for easy identification

## Setup

1. **Create a configuration file.**

    Copy the `config.example.yaml` to `config.yaml` and update the values:

    ```bash
    cp config.example.yaml config.yaml
    ```

    Then, edit `config.yaml`:

    ### Global Configuration

    - `port`: The port the exporter will listen on (e.g., `9090`).

    ### Networks

    Each entry under the `networks` map defines a network to monitor. Example:

    ```yaml
    networks:
      solana_mainnet:
        rpc_url: "https://api.mainnet-beta.solana.com"
        validators:
          - vote_address: "YourVoteAccount"
            identity_address: "YourIdentityAccount"
        labels:
          stage: mainnet
          environment: production
    ```

    For every network provide:
    - `rpc_url`: The RPC endpoint for the network.
    - `validators`: A list of validator objects with `vote_address` and `identity_address` keys.
    - `labels` _(optional)_: Additional key/value pairs that will be attached to every metric for that network.
    - `delinquency_seconds` _(optional, default `60`)_: A validator whose last vote is older than this is reported as delinquent (`solana_voting_status` = 0). It is converted to slots every loop iteration with the measured slot time (`max(32, round(seconds * 1000 / slot_time_ms))`), so it is 300 slots at 200 ms slots and 150 slots at 400 ms slots.
    - `reward_window_seconds` _(optional, default `900`, minimum `60`)_: Sliding window over which `solana_epoch_credits_rate_ratio`, `solana_epoch_credits_rate_percentile` and `solana_leader_slots_in_window` are computed. The exporter keeps one snapshot of every staked vote account per loop iteration (30 s) for this window: about 700 accounts x 30 snapshots at the default.
    - `recent_window_seconds` _(optional, default `600`)_: How far back (in wall-clock time) leader slots are fetched for `solana_epoch_block_rewards`.

    The exporter automatically adds `network` and `vote_account` labels. If you supply a `network` entry inside `labels`, it overrides the default value derived from the config key. Note that you can also set labels for each validator, which overrides the values configured at the network level.

## Running the Exporter

You have two options to run the exporter: direct build or Docker container.

### Option 1: Direct Build

1. **Install Rust** (if not already installed):

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Build the exporter**:

   ```bash
   cargo build --release
   ```

3. **Run the exporter**:

   ```bash
   ./target/release/solana-validator-exporter --config-file config.yaml
   ```

### Option 2: Docker Container

1. **Build the Docker image**:

   ```bash
   docker build -t solana-validator-exporter -f docker/Dockerfile .
   ```

2. **Run the container**:

   ```bash
   docker run -d \
     --name solana-validator-exporter \
     -p 9090:9090 \
     -v $(pwd)/config.yaml:/home/exporter/config.yaml \
     solana-validator-exporter --config-file /home/exporter/config.yaml
   ```

## Verifying the Exporter

After running either method, you can verify the exporter is working by accessing the metrics endpoint:

```bash
curl http://localhost:9090/metrics
```

The exporter will expose metrics on the port specified in your configuration file (e.g., `http://localhost:9090/metrics`).

## Metrics

All metrics are gauges and carry the labels described in the next section.

### Cluster and epoch

| Metric | Description |
| --- | --- |
| `solana_slot` | Current slot of the cluster, as seen by the configured RPC node. |
| `solana_epoch` | Current epoch. |
| `solana_epoch_progress` | Progress through the epoch, in basis points (0-10000). |
| `solana_slot_time_ms` | Average slot time measured from the last 60 performance samples. Every slot/time conversion in the exporter uses this value; when the samples contain no slots it falls back to 200 ms and logs a warning. |
| `solana_usd_price` | SOL/USD price from Kraken, multiplied by 1e5. |

### Validator state

| Metric | Description |
| --- | --- |
| `solana_voting_status` | 1 when the vote account is current, 0 when delinquent (see `delinquency_seconds`), -1 when not found in the RPC response. |
| `solana_vote_distance_slots` | Current slot minus the vote account's `lastVote`. Taken from the same RPC node in the same loop iteration as `solana_slot`, so alerting thresholds can be chosen in Grafana without an exporter release. |
| `solana_root_distance_slots` | Current slot minus the vote account's `rootSlot`, same source as above. |
| `solana_vote_account_epoch` | Epoch of the newest usable `epochCredits` entry. It lags `solana_epoch` when the vote account or the RPC node is stale. |
| `solana_bls_pubkey_registered` | 1 when the vote account's state contains a BLS pubkey (`blsPubkeyCompressed`), else 0. Admission to the Alpenglow validator set requires it. Polled once per epoch; 0 with a one-time warning when the RPC node does not report the field. |
| `solana_stake{stake_type=...}` | Stake delegated to the vote account, in lamports, by `activated`, `activating`, `deactivating`, `locked`, and account counts `activated_accounts`, `activating_accounts`, `deactivating_accounts`. |
| `solana_identity_balance` | Identity account balance in lamports. |
| `solana_vote_account_balance` | Vote account balance in lamports. |

### Vote rewards (Alpenglow)

Since Alpenglow (mainnet epoch 1064), `epochCredits` entries are `[epoch, cumulative_reward_lamports, previous_cumulative]`, so the per-epoch delta is reward lamports and is almost proportional to the activated stake. The metrics below therefore compare credits **per lamport of stake** against the cluster. A validator can be non-delinquent and still earn well below the cluster when its votes do not reach the leaders inside the aggregation window; these metrics are what catches that.

| Metric | Description |
| --- | --- |
| `solana_epoch_credits` | Credits (reward lamports) earned in the current epoch: the `credits - prev_credits` delta of the entry whose epoch equals the current epoch. 0 when the account has no entry for the current epoch. |
| `solana_epoch_credits_per_stake_ratio` | Our epoch-to-date credits per activated stake, divided by the cluster median of the same quantity over all vote accounts with `activatedStake > 0` and a current-epoch entry. 1.0 means median. |
| `solana_epoch_credits_percentile` | Percentage (0-100) of staked vote accounts whose credits per stake is strictly below ours. Accounts without a current-epoch entry count as 0 earners. |
| `solana_epoch_credits_rate_ratio` | Our credits per stake earned over the last `reward_window_seconds`, divided by the **median of the per-account credits per stake earned over the same window, over accounts that had no leader slot inside the window** (and were present at both ends of it with a positive increase). When fewer than 50 such accounts exist, the 10th percentile of all accounts is used instead and a warning is logged once per epoch. 1.0 means median; a problem that starts mid-epoch shows immediately. This is the metric to alert on, for values **below** 1. Our own leader slots inflate it for one window after each of our leader windows (see `solana_leader_slots_in_window`). Absent until the window holds at least two snapshots at least 60 s apart, and reset at epoch boundaries. |
| `solana_epoch_credits_rate_percentile` | Percentage (0-100) of the same windowed population whose credits per stake earned over the window is strictly below ours. **Informational only, do not alert on it.** Every validator whose votes land in time earns the same reward per unit of stake, so the windowed values form a tight cluster at the pure voting rate; inside that cluster the ordering is noise and a healthy validator (ratio ~1.0) can read anywhere from roughly 10 to 60. It is meaningful only at the extremes, e.g. below 5 confirms we are in the lower tail. |
| `solana_leader_slots_in_window` | Number of our scheduled leader slots whose slot number falls inside the reward window. When it is non-zero, `solana_epoch_credits_rate_ratio` spikes above 1 because leader rewards for vote aggregates land in `epochCredits` too. Informational. |
| `solana_rate_reference_accounts` | Size of the reference population of `solana_epoch_credits_rate_ratio`: staked accounts that earned credits in the window and had no leader slot inside it. Below 50 the ratio is using the p10-of-all-accounts fallback. |
| `solana_vote_credit_rank` | **Deprecated since Alpenglow.** Rank of the vote account by raw epoch credits among current vote accounts. Because credits are now proportional to stake this is effectively a rank by stake. It is still emitted for one release so dashboards do not break and will be removed in the next release; switch alerts and dashboards to `solana_epoch_credits_rate_ratio` / `solana_epoch_credits_percentile`. |

Under SIMD-0326 the leader that posts a vote aggregate is paid the same amount as each voter it includes, and both land in `epochCredits`. Two consequences for reading these metrics:

- The epoch-to-date metrics (`solana_epoch_credits_per_stake_ratio`, `solana_epoch_credits_percentile`) are dominated early in the epoch by **when** a validator's leader windows fall: a validator whose first leader window is late reads below 1 until then, and above 1 right after it. They converge over the epoch.
- The windowed ratio must not be divided by the growth of the cluster median *cumulative* value, nor by a statistic over *all* accounts' windowed increases. One leader window pays a mid-size validator roughly as much as 15 minutes of voting, and mainnet hands out about 1125 leader windows per 15 minutes across ~660 validators, so over such a window more than half of the accounts carry a leader bump and the median of all accounts roughly doubles (testnet's stake is more concentrated, so there it does not). Both formulas read about 0.5 for a healthy mainnet validator. The stable reference is the voting rate of accounts with **no leader slot inside the window**, whose p10..p90 spread is within 1 % of their median on both networks. The exporter therefore keeps one snapshot per loop of every staked account's credits and identity, fetches the epoch's leader schedule once per epoch, and divides by the median over the zero-leader accounts. The window length does not matter for this reference.

All vote-related metrics are derived from a single `getVoteAccounts` call per loop iteration (with `keepUnstakedDelinquents: true`), so they are consistent with each other. The `u64::MAX` migration marker that some vote accounts carry as their last `epochCredits` entry is skipped and never affects the statistics or the `/metrics` endpoint.

### Block production and rewards

| Metric | Description |
| --- | --- |
| `solana_blocks{block_type=...}` | `scheduled`: leader slots in the current epoch; `produced`, `skipped` and `total` from `getBlockProduction`. 0 scheduled slots with non-zero activated stake means the validator was not admitted to the leader schedule (VAT or missing BLS pubkey). |
| `solana_ms_to_next_slot` | Milliseconds until the next leader slot, computed with `solana_slot_time_ms`; -1 when there is no upcoming leader slot. |
| `solana_epoch_block_rewards` | Sum of block rewards (lamports) earned in leader slots this epoch. Slots within the last `recent_window_seconds` are fetched. |
| `solana_last_block_rewards` | Average of the last four non-zero block rewards. |
| `solana_jito_tips` | Balance of the Jito tip distribution account for the current epoch. |

## Metrics Labels

All metrics include the following labels for easy filtering and identification:

- `network`: Defaults to the network name from the config (override with `labels.network`).
- `vote_account`: The validator's vote account public key.
- Any additional key/value pairs provided in the network's `labels` map.

Example metric with labels:

```
solana_slot{network="solana_mainnet",vote_account="YourVoteAccount...",stage="mainnet"} 123456789
```

This makes it straightforward to build Grafana dashboards or alerts scoped to specific environments, regions, or clusters.
