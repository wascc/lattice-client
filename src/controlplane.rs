use std::collections::HashMap;

use crate::Client;

pub const CPLANE_PREFIX: &str = "control";
pub const AUCTION_REQ: &str = "auction.request";
pub const PROVIDER_AUCTION_REQ: &str = "provauction.request";
pub const LAUNCH_ACTOR: &str = "actor.launch";
pub const LAUNCH_PROVIDER: &str = "provider.launch";
pub const TERMINATE_ACTOR: &str = "actor.terminate";
pub const TERMINATE_PROVIDER: &str = "provider.terminate";

/// A request sent out to all listening hosts on the bus to launch a given
/// capability provider with the set of constraints
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ProviderAuctionRequest {
    pub provider_ref: String,
    pub binding_name: String,
    pub constraints: HashMap<String, String>,
}

impl ProviderAuctionRequest {
    /// Creates a new auction request for the provider stored at the given OCI
    /// registry reference with the given constraints
    pub fn new(
        provider_ref: &str,
        binding_name: &str,
        constraints: HashMap<String, String>,
    ) -> ProviderAuctionRequest {
        ProviderAuctionRequest {
            provider_ref: provider_ref.to_string(),
            binding_name: binding_name.to_string(),
            constraints,
        }
    }
}

/// The response submitted by a host that confirms that it has sufficient resources
/// and meets the constraints specified in the request to launch the indicated provider
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ProviderAuctionResponse {
    pub host_id: String,
    pub provider_ref: String,
}

/// A request sent out to all listening hosts on the bus to launch a given actor
/// with a set of given constraints
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchAuctionRequest {
    pub actor_id: String,
    pub constraints: HashMap<String, String>,
}

impl LaunchAuctionRequest {
    pub fn new(actor: &str, constraints: HashMap<String, String>) -> LaunchAuctionRequest {
        LaunchAuctionRequest {
            actor_id: actor.to_string(),
            constraints,
        }
    }
}

/// A command sent to a specific host to shut down a given actor
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct TerminateCommand {
    pub actor_id: String,
}

/// A command sent to a specific host instructing it to load and start a given actor
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchCommand {
    pub actor_id: String,
}

/// A command sent to a specific host instructing it to load and start a given provider
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchProviderCommand {
    pub provider_ref: String,
    pub binding_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchAck {
    pub actor_id: String,
    pub host: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ProviderLaunchAck {
    pub provider_ref: String,
    pub host: String,
}

/// A command sent to a specific host to terminate a given provider
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct TerminateProviderCommand {
    pub provider_ref: String,
}

/// The response submitted by a host that confirms that it has sufficient resources
/// and meets the constraints specified in the request
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchAuctionResponse {
    pub host_id: String,
}

impl Client {
    pub(crate) fn gen_launch_actor_subject(&self, host: &str) -> String {
        self.gen_subject(&format!("{}.{}.{}", CPLANE_PREFIX, host, LAUNCH_ACTOR))
        // e.g. wasmbus.control.Nxxxx.actor.launch
    }
    pub(crate) fn gen_terminate_actor_subject(&self, host: &str) -> String {
        self.gen_subject(&format!("{}.{}.{}", CPLANE_PREFIX, host, TERMINATE_ACTOR))
        // e.g. wasmbus.control.Nxxxx.actor.terminate
    }

    pub(crate) fn gen_launch_provider_subject(&self, host: &str) -> String {
        self.gen_subject(&format!("{}.{}.{}", CPLANE_PREFIX, host, LAUNCH_PROVIDER))
    }
    pub(crate) fn gen_terminate_provider_subject(&self, host: &str) -> String {
        self.gen_subject(&format!(
            "{}.{}.{}",
            CPLANE_PREFIX, host, TERMINATE_PROVIDER
        ))
    }
}
