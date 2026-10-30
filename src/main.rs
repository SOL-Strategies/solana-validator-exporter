mod metrics;
mod solana;
use axum::routing::get;
use axum::Router;
use clap::Parser;
use env_logger::Env;
use log::info;
use metrics::exporter::{metrics_handler, CollectorSettings};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Prometheus exporter for solana validators
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// Path to config file
    #[arg(long)]
    config_file: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    port: u16,
    networks: BTreeMap<String, NetworkConfig>,
}

/// Wall-clock window after which a validator whose last vote is older than this
/// is reported as delinquent. Converted to slots with the measured slot time.
const DEFAULT_DELINQUENCY_SECONDS: u64 = 60;
/// Sliding window over which `solana_epoch_credits_rate_ratio` is computed.
const DEFAULT_REWARD_WINDOW_SECONDS: u64 = 900;
/// Lookback window for fetching block rewards of recent leader slots.
const DEFAULT_RECENT_WINDOW_SECONDS: u64 = 600;

fn default_delinquency_seconds() -> u64 {
    DEFAULT_DELINQUENCY_SECONDS
}

fn default_reward_window_seconds() -> u64 {
    DEFAULT_REWARD_WINDOW_SECONDS
}

fn default_recent_window_seconds() -> u64 {
    DEFAULT_RECENT_WINDOW_SECONDS
}

fn validate_reward_window_seconds(
    network_name: &str,
    reward_window_seconds: u64,
) -> Result<(), String> {
    if reward_window_seconds < solana::vote_accounts::MIN_RATE_WINDOW_SECONDS {
        return Err(format!(
            "network `{network_name}`: reward_window_seconds must be at least {}",
            solana::vote_accounts::MIN_RATE_WINDOW_SECONDS
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct NetworkConfig {
    rpc_url: String,
    validators: Vec<NetworkValidator>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default = "default_delinquency_seconds")]
    delinquency_seconds: u64,
    #[serde(default = "default_reward_window_seconds")]
    reward_window_seconds: u64,
    #[serde(default = "default_recent_window_seconds")]
    recent_window_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct NetworkValidator {
    vote_address: String,
    identity_address: String,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct ValidatorConfig {
    network_key: String,
    rpc_url: String,
    vote_account: String,
    identity_account: String,
    labels: BTreeMap<String, String>,
    settings: CollectorSettings,
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    let file = File::open(args.config_file).unwrap();
    let reader = BufReader::new(file);
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Parse the YAML file
    let config: Config = serde_yaml::from_reader(reader).expect("Error parsing yaml file");

    // Validate configuration and build validator configs
    let mut validator_configs = Vec::new();

    if config.networks.is_empty() {
        panic!("At least one network must be provided under the `networks` configuration section");
    }

    for (network_name, network_config) in &config.networks {
        validate_reward_window_seconds(network_name, network_config.reward_window_seconds)
            .unwrap_or_else(|error| panic!("{error}"));

        for validator in &network_config.validators {
            let mut labels: BTreeMap<String, String> = network_config
                .labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();

            labels
                .entry("network".to_string())
                .or_insert(network_name.clone());
            labels.insert("vote_account".to_string(), validator.vote_address.clone());

            // Iterate validator.labels and insert into labels, overriding any existing keys
            labels.extend(validator.labels.clone());

            validator_configs.push(ValidatorConfig {
                network_key: network_name.clone(),
                rpc_url: network_config.rpc_url.clone(),
                vote_account: validator.vote_address.clone(),
                identity_account: validator.identity_address.clone(),
                labels,
                settings: CollectorSettings {
                    delinquency_seconds: network_config.delinquency_seconds,
                    reward_window_seconds: network_config.reward_window_seconds,
                    recent_window_seconds: network_config.recent_window_seconds,
                },
            });
        }
    }

    if validator_configs.is_empty() {
        panic!("At least one validator must be configured under `networks`");
    }

    info!(
        "Starting exporter with {} validator(s)!",
        validator_configs.len()
    );

    // Create shared metrics registry
    let shared_state = Arc::new(Mutex::new(metrics::exporter::AppState {
        registry: prometheus_client::registry::Registry::default(),
    }));

    // Create and start metrics collection for each validator
    let mut handles = Vec::new();
    for validator_config in validator_configs {
        let ValidatorConfig {
            network_key,
            rpc_url,
            vote_account,
            identity_account,
            labels,
            settings,
        } = validator_config;

        info!(
            "Starting metrics collection for network `{}` validator: {}",
            &network_key, &vote_account
        );

        let metrics = Arc::new(metrics::exporter::Metrics::new(
            rpc_url,
            identity_account,
            vote_account,
            labels,
            settings,
        ));

        // Initialize metrics in shared registry
        metrics.init_registry(shared_state.clone()).await;

        // Start metrics collection loop
        let handle = metrics.clone().run_loop();
        handles.push(handle);
    }

    let router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(shared_state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();

    axum::serve(listener, router).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_network_keys_are_optional_with_defaults() {
        let yaml = r#"
port: 9099
networks:
  solana_mainnet:
    rpc_url: "https://api.mainnet-beta.solana.com"
    validators:
      - vote_address: "vote"
        identity_address: "identity"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let network = &config.networks["solana_mainnet"];
        assert_eq!(network.delinquency_seconds, 60);
        assert_eq!(network.reward_window_seconds, 900);
        assert_eq!(network.recent_window_seconds, 600);
    }

    #[test]
    fn new_network_keys_are_read() {
        let yaml = r#"
port: 9099
networks:
  solana_mainnet:
    rpc_url: "https://api.mainnet-beta.solana.com"
    delinquency_seconds: 45
    reward_window_seconds: 1200
    recent_window_seconds: 300
    validators:
      - vote_address: "vote"
        identity_address: "identity"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let network = &config.networks["solana_mainnet"];
        assert_eq!(network.delinquency_seconds, 45);
        assert_eq!(network.reward_window_seconds, 1200);
        assert_eq!(network.recent_window_seconds, 300);
    }

    #[test]
    fn reward_window_must_cover_the_minimum_rate_span() {
        assert!(validate_reward_window_seconds("mainnet", 60).is_ok());
        assert_eq!(
            validate_reward_window_seconds("mainnet", 59),
            Err("network `mainnet`: reward_window_seconds must be at least 60".to_string())
        );
    }
}
