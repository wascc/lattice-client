use crate::Client;
use std::collections::HashMap;

pub const CPLANE_PREFIX: &str = "control";
pub const AUCTION_REQ: &str = "auction.request";
pub const LAUNCH_ACTOR: &str = "actor.launch";
pub const TERMINATE_ACTOR: &str = "actor.terminate";

/// A request sent out to all listening hosts on the bus to launch a given actor
/// with a set of given constraints
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchAuctionRequest {
    pub actor_id: String,
    pub revision: u32,
    pub constraints: HashMap<String, String>,
}

impl LaunchAuctionRequest {
    pub fn new(
        actor: &str,
        revision: u32,
        constraints: HashMap<String, String>,
    ) -> LaunchAuctionRequest {
        LaunchAuctionRequest {
            actor_id: actor.to_string(),
            revision,
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
    pub revision: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LaunchAck {
    pub actor_id: String,
    pub host: String,
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
}
