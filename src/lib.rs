// Copyright 2015-2020 Capital One Services, LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Lattice Client
//!
//! This library provides a client that communicates with a waSCC lattice using
//! the lattice protocol over the NATS message broker. All waSCC hosts compiled
//! in lattice mode have the ability to automatically form self-healing, self-managing
//! infrastructure-agnostic clusters called **lattices**.

#[macro_use]
extern crate serde;

extern crate log;

use std::{collections::HashMap, path::PathBuf, time::Duration};
use wascap::prelude::*;

pub const INVENTORY_ACTORS: &str = "wasmbus.inventory.actors";
pub const INVENTORY_HOSTS: &str = "wasmbus.inventory.hosts";
pub const INVENTORY_BINDINGS: &str = "wasmbus.inventory.bindings";
pub const INVENTORY_CAPABILITIES: &str = "wasmbus.inventory.capabilities";

/// A response to a lattice probe for inventory. Note that these responses are returned
/// through regular (non-queue) subscriptions via a scatter-gather like pattern, so the
/// client is responsible for aggregating many of these replies.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum InventoryResponse {
    /// A single host probe response
    Host(String),
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
    timeout: Duration,
}

impl Client {
    /// Creates a new lattice client, connecting to the NATS server at the
    /// given host with an optional set of credentials (JWT auth)
    pub fn new(host: &str, credsfile: Option<PathBuf>, call_timeout: Duration) -> Self {
        Client {
            nc: get_connection(host, credsfile),
            timeout: call_timeout,
        }
    }

    /// Retrieves the list of all hosts running within the lattice. If it takes a host longer
    /// than the call timeout period to reply to the probe, it will not be included in the list
    /// of hosts.
    pub fn get_hosts(&self) -> std::result::Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut hosts = vec![];
        let sub = self.nc.request_multi(INVENTORY_HOSTS, &[])?;
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

        let sub = self.nc.request_multi(INVENTORY_BINDINGS, &[])?;
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

        let sub = self.nc.request_multi(INVENTORY_ACTORS, &[])?;
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
        let sub = self.nc.request_multi(INVENTORY_CAPABILITIES, &[])?;
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
}

fn get_connection(host: &str, credsfile: Option<PathBuf>) -> nats::Connection {
    let mut opts = if let Some(creds) = credsfile {
        nats::ConnectionOptions::with_credentials(creds)
    } else {
        nats::ConnectionOptions::new()
    };
    opts = opts.with_name("waSCC Lattice");
    opts.connect(host).unwrap()
}
