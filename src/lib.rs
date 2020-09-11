//! # Lattice Client
//!
//! This library provides a client that communicates with a waSCC lattice using
//! the lattice protocol over the NATS message broker. All waSCC hosts compiled
//! in lattice mode have the ability to automatically form self-healing, self-managing
//! infrastructure-agnostic clusters called [lattices](https://wascc.dev/docs/lattice/overview/)

extern crate log;
#[macro_use]
extern crate serde;

use std::{collections::HashMap, path::PathBuf, time::Duration};

use controlplane::{
    LaunchAck, LaunchAuctionRequest, LaunchAuctionResponse, LaunchCommand, TerminateCommand,
};
use crossbeam::Sender;
use wascap::prelude::*;

pub use events::{BusEvent, CloudEvent};

pub mod controlplane;
mod events;

pub const INVENTORY_ACTORS: &str = "inventory.actors";
pub const INVENTORY_HOSTS: &str = "inventory.hosts";
pub const INVENTORY_BINDINGS: &str = "inventory.bindings";
pub const INVENTORY_CAPABILITIES: &str = "inventory.capabilities";
pub const EVENTS: &str = "events";
const AUCTION_TIMEOUT_SECONDS: u64 = 5;

/// A response to a lattice probe for inventory. Note that these responses are returned
/// through regular (non-queue) subscriptions via a scatter-gather like pattern, so the
/// client is responsible for aggregating many of these replies.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum InventoryResponse {
    /// A single host probe response
    Host(HostProfile),
    /// A list of all registered actors within a host
    Actors {
        host: String,
        actors: Vec<Claims<Actor>>,
    },
    /// A list of configuration bindings of actors originating from the given host
    Bindings {
        host: String,
        bindings: Vec<Binding>,
    },
    /// A list of capability providers currently running within the given host
    Capabilities {
        host: String,
        capabilities: Vec<HostedCapability>,
    },
}

/// An overview of host information
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct HostProfile {
    /// The public key (subject) of the host
    pub id: String,
    /// The host's labels
    pub labels: HashMap<String, String>,
    /// Host uptime in milliseconds
    pub uptime_ms: u128,
}

/// Represents an instance of a capability, which is a binding name and
/// the capability descriptor
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HostedCapability {
    pub binding_name: String,
    pub descriptor: wascc_codec::capabilities::CapabilityDescriptor,
}

/// Represents a single configuration binding from an actor to a capability ID and binding
/// name, with the specified configuration values.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Binding {
    pub actor: String,
    pub capability_id: String,
    pub binding_name: String,
    pub configuration: HashMap<String, String>,
}

/// A client for interacting with the lattice
pub struct Client {
    nc: nats::Connection,
    namespace: Option<String>,
    timeout: Duration,
}

impl Client {
    /// Creates a new lattice client, connecting to the NATS server at the
    /// given host with an optional set of credentials (JWT auth)
    pub fn new(
        host: &str,
        credsfile: Option<PathBuf>,
        call_timeout: Duration,
        namespace: Option<String>,
    ) -> Self {
        Client {
            nc: get_connection(host, credsfile),
            timeout: call_timeout,
            namespace,
        }
    }

    /// Retrieves the list of all hosts running within the lattice. If it takes a host longer
    /// than the call timeout period to reply to the probe, it will not be included in the list
    /// of hosts.
    pub fn get_hosts(&self) -> std::result::Result<Vec<HostProfile>, Box<dyn std::error::Error>> {
        let mut hosts = vec![];
        let sub = self
            .nc
            .request_multi(self.gen_subject(INVENTORY_HOSTS).as_ref(), &[])?;
        for msg in sub.timeout_iter(self.timeout) {
            let ir: InventoryResponse = serde_json::from_slice(&msg.data)?;
            if let InventoryResponse::Host(h) = ir {
                hosts.push(h);
            }
        }
        Ok(hosts)
    }

    /// Retrieves a list of all bindings from actors to capabilities within the lattice (provided
    /// the host responds to the probe within the client timeout period)
    pub fn get_bindings(
        &self,
    ) -> std::result::Result<HashMap<String, Vec<Binding>>, Box<dyn std::error::Error>> {
        let mut host_bindings = HashMap::new();

        let sub = self
            .nc
            .request_multi(self.gen_subject(INVENTORY_BINDINGS).as_ref(), &[])?;
        for msg in sub.timeout_iter(self.timeout) {
            let ir: InventoryResponse = serde_json::from_slice(&msg.data)?;
            if let InventoryResponse::Bindings { bindings: b, host } = ir {
                host_bindings
                    .entry(host)
                    .and_modify(|e: &mut Vec<Binding>| e.extend_from_slice(&b))
                    .or_insert(b.clone());
            }
        }
        Ok(host_bindings)
    }

    /// Retrieves the list of all actors currently running within the lattice (as discovered within
    /// the client timeout period)
    pub fn get_actors(
        &self,
    ) -> std::result::Result<HashMap<String, Vec<Claims<Actor>>>, Box<dyn std::error::Error>> {
        let mut host_actors = HashMap::new();

        let sub = self
            .nc
            .request_multi(self.gen_subject(INVENTORY_ACTORS).as_ref(), &[])?;
        for msg in sub.timeout_iter(self.timeout) {
            let ir: InventoryResponse = serde_json::from_slice(&msg.data)?;
            if let InventoryResponse::Actors { host, actors } = ir {
                host_actors
                    .entry(host)
                    .and_modify(|e: &mut Vec<Claims<Actor>>| e.extend_from_slice(&actors))
                    .or_insert(actors.clone());
            }
        }
        Ok(host_actors)
    }

    /// Retrieves the list of all capabilities within the lattice (discovery limited by the client timeout period)
    pub fn get_capabilities(
        &self,
    ) -> std::result::Result<HashMap<String, Vec<HostedCapability>>, Box<dyn std::error::Error>>
    {
        let mut host_caps = HashMap::new();
        let sub = self
            .nc
            .request_multi(self.gen_subject(INVENTORY_CAPABILITIES).as_ref(), &[])?;
        for msg in sub.timeout_iter(self.timeout) {
            let ir: InventoryResponse = serde_json::from_slice(&msg.data)?;
            if let InventoryResponse::Capabilities { host, capabilities } = ir {
                host_caps
                    .entry(host)
                    .and_modify(|e: &mut Vec<HostedCapability>| e.extend_from_slice(&capabilities))
                    .or_insert(capabilities.clone());
            }
        }
        Ok(host_caps)
    }

    /// Watches the lattice for bus events. This will create a subscription in a background thread, so callers
    /// are responsible for ensuring their process remains alive however long is appropriate. Pass the sender
    /// half of a channel to receive the events
    pub fn watch_events(&self, sender: Sender<BusEvent>) -> Result<(), Box<dyn std::error::Error>> {
        let _sub = self
            .nc
            .subscribe(self.gen_subject(EVENTS).as_ref())?
            .with_handler(move |msg| {
                let ce: CloudEvent = serde_json::from_slice(&msg.data).unwrap();
                let be: BusEvent = serde_json::from_str(&ce.data).unwrap();
                let _ = sender.send(be);
                Ok(())
            });
        Ok(())
    }

    /// Performs an auction among all hosts on the lattice, requesting that the given actor be launched (loaded+started)
    /// on a suitable host as described by the set of constraints. Only hosts that believe they can launch the actor
    /// will reply. In other words, there will be no negative responses in the result vector, only a list of suitable
    /// hosts.
    pub fn perform_launch_auction(
        &self,
        actor_id: &str,
        revision: u32,
        constraints: HashMap<String, String>,
    ) -> Result<Vec<LaunchAuctionResponse>, Box<dyn std::error::Error>> {
        let mut results = vec![];
        let req = LaunchAuctionRequest::new(actor_id, revision, constraints);
        let sub = self.nc.request_multi(
            self.gen_subject(&format!(
                "{}.{}",
                controlplane::CPLANE_PREFIX,
                controlplane::AUCTION_REQ
            ))
            .as_ref(),
            &serde_json::to_vec(&req)?,
        )?;
        for msg in sub.timeout_iter(self.timeout) {
            let resp: LaunchAuctionResponse = serde_json::from_slice(&msg.data)?;
            results.push(resp);
        }
        Ok(results)
    }

    /// After collecting the results of a launch auction, a "winner" from among the hosts can be selected and
    /// told to launch a given actor. Note that the actor's bytes must reside in a connected Gantry instance, and
    /// this function does _not_ confirm successful launch, only that the target host acknowledged the request
    /// to launch.
    pub fn launch_actor_on_host(
        &self,
        actor_id: &str,
        revision: u32,
        host_id: &str,
    ) -> Result<LaunchAck, Box<dyn std::error::Error>> {
        let msg = LaunchCommand {
            actor_id: actor_id.to_string(),
            revision,
        };
        let ack: LaunchAck = serde_json::from_slice(
            &self
                .nc
                .request_timeout(
                    &self.gen_launch_actor_subject(host_id),
                    &serde_json::to_vec(&msg)?,
                    Duration::from_secs(AUCTION_TIMEOUT_SECONDS),
                )?
                .data,
        )?;
        Ok(ack)
    }

    /// Sends a command to the specified host telling it to terminate an actor. The success of this command indicates
    /// a successful publication, and not necessarily a successful remote actor termination. Monitor the lattice
    /// events to see if the actor was successfully terminated
    pub fn stop_actor_on_host(
        &self,
        actor_id: &str,
        host_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let msg = TerminateCommand {
            actor_id: actor_id.to_string(),
        };
        self.nc.publish(
            &self.gen_terminate_actor_subject(host_id),
            &serde_json::to_vec(&msg)?,
        )?;
        let _ = self.nc.flush();
        Ok(())
    }

    fn gen_subject(&self, subject: &str) -> String {
        match self.namespace.as_ref() {
            Some(s) => format!("{}.wasmbus.{}", s, subject),
            None => format!("wasmbus.{}", subject),
        }
    }
}

fn get_connection(host: &str, credsfile: Option<PathBuf>) -> nats::Connection {
    let mut opts = if let Some(creds) = credsfile {
        nats::Options::with_credentials(creds)
    } else {
        nats::Options::new()
    };
    opts = opts.with_name("waSCC Lattice");
    opts.connect(host).unwrap()
}
