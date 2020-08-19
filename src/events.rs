use chrono::prelude::*;
use std::fmt;
use uuid::Uuid;

pub const BUS_EVENT_SUBJECT: &str = "wasmbus.events";

/// Represents an event that may occur on a bus of connected hosts. Timestamps, identifiers, and
/// other metadata will be provided by a [CloudEvent](struct.CloudEvent.html) envelope.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum BusEvent {
    /// A host process has fully started and is ready for work
    HostStarted(String),
    /// A host has stopped on a predictable manner
    HostStopped(String),
    /// An actor has begun the loading/parsing phase
    ActorStarting { actor: String, host: String },
    /// An actor has started and is ready to receive messages
    ActorStarted { actor: String, host: String },
    /// An actor has stopped in a predictable manner
    ActorStopped { actor: String, host: String },
    /// A live update/hot swap has begun on this actor
    ActorUpdating { actor: String, host: String },
    /// A live update/hot swap has completed (or failed)
    ActorUpdateComplete {
        actor: String,
        success: bool,
        host: String,
    },
    /// A capability provider has been loaded on a host
    ProviderLoaded {
        capid: String,
        instance_name: String,
        host: String,
    },
    /// A capability provider has been removed from a host
    ProviderRemoved {
        capid: String,
        instance_name: String,
        host: String,
    },
    /// A binding between an actor and a named capability provider instance was created. For security reasons the raw data of
    /// the individual bound configuration values is not published in this event and must be queried via lattice protocol
    ActorBindingCreated {
        host: String,
        actor: String,
        capid: String,
        instance_name: String,
    },
    /// A binding was removed
    ActorBindingRemoved {
        host: String,
        actor: String,
        capid: String,
        instance_name: String,
    },
    /// A previously unhealthy actor became healthy. This is a higher-order event, not generated from inside wascc host
    ActorBecameHealthy { actor: String, host: String },
    /// A previously healthy actor became unhealthy. This is a higher-order event, not generated from inside wascc host
    ActorBecameUnhealthy { actor: String, host: String },
}

const EVENT_TYPE_PREFIX: &str = "wasmbus.events";

impl BusEvent {
    pub fn event_type(&self) -> String {
        use BusEvent::*;

        let suffix = match self {
            HostStarted(_) => "host_started",
            HostStopped(_) => "host_stopped",
            ActorStarting { .. } => "actor_starting",
            ActorStarted { .. } => "actor_started",
            ActorStopped { .. } => "actor_stopped",
            ActorUpdating { .. } => "actor_updating",
            ActorUpdateComplete { .. } => "actor_update_complete",
            ProviderLoaded { .. } => "provider_loaded",
            ProviderRemoved { .. } => "provider_removed",
            ActorBindingCreated { .. } => "actor_binding_created",
            ActorBindingRemoved { .. } => "actor_binding_removed",
            ActorBecameHealthy { .. } => "actor_became_healthy",
            ActorBecameUnhealthy { .. } => "actor_became_unhealthy",
        };
        format!("{}.{}", EVENT_TYPE_PREFIX, suffix)
    }

    pub fn subject(&self) -> String {
        use BusEvent::*;

        match self {
            HostStarted(h) => h.to_string(),
            HostStopped(h) => h.to_string(),
            ActorStarting { actor, .. }
            | ActorStarted { actor, .. }
            | ActorStopped { actor, .. }
            | ActorUpdating { actor, .. }
            | ActorUpdateComplete { actor, .. } => actor.to_string(),
            ProviderLoaded {
                capid,
                instance_name,
                ..
            }
            | ProviderRemoved {
                capid,
                instance_name,
                ..
            } => format!("{}.{}", capid, instance_name),
            ActorBindingCreated {
                actor,
                capid,
                instance_name,
                ..
            }
            | ActorBindingRemoved {
                actor,
                capid,
                instance_name,
                ..
            } => format!("{}.{}.{}", actor, capid, instance_name),
            ActorBecameHealthy { actor, .. } | ActorBecameUnhealthy { actor, .. } => {
                actor.to_string()
            }
        }
    }
}

impl fmt::Display for BusEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BusEvent::*;
        match self {
            HostStarted(h) => write!(f, "[{}] Host started", h),
            HostStopped(h) => write!(f, "[{}] Host stopped", h),
            ActorStarting { actor, host } => write!(f, "[{}] Actor {} starting", host, actor),
            ActorStarted { actor, host } => write!(f, "[{}] Actor {} started", host, actor),
            ActorStopped { actor, host } => write!(f, "[{}] Actor {} stopped", host, actor),
            ActorUpdating { actor, host } => write!(f, "[{}] Actor {} updating", host, actor),
            ActorUpdateComplete {
                actor,
                host,
                success,
            } => write!(
                f,
                "[{}] Actor {} update {}",
                host,
                actor,
                if *success { "succeeded" } else { "failed" }
            ),
            ProviderLoaded {
                capid,
                instance_name,
                host,
            } => write!(f, "[{}] Provider {},{} loaded", host, capid, instance_name),
            ProviderRemoved {
                capid,
                host,
                instance_name,
            } => write!(f, "[{}] Provider {},{} removed", host, capid, instance_name),
            ActorBindingCreated {
                actor,
                capid,
                instance_name,
                host,
            } => write!(
                f,
                "[{}] Actor {} bound to {},{}",
                host, actor, capid, instance_name
            ),
            ActorBindingRemoved {
                actor,
                capid,
                instance_name,
                host,
            } => write!(
                f,
                "[{}] Actor {} un-bound from {},{}",
                host, actor, capid, instance_name
            ),
            ActorBecameHealthy { host, actor } => {
                write!(f, "[{}] Actor {} became healthy", host, actor)
            }
            ActorBecameUnhealthy { host, actor } => {
                write!(f, "[{}] Actor {} became unhealthy", host, actor)
            }
        }
    }
}

/// An envelope for an event that corresponds to the [CloudEvents 1.0 Spec](https://github.com/cloudevents/spec/blob/v1.0/spec.md)
/// with the intent to be serialized in the JSON format.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CloudEvent {
    #[serde(rename = "specversion")]
    pub cloud_events_version: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "typeversion")]
    pub event_type_version: String,
    pub source: String, // URI
    #[serde(rename = "id")]
    pub event_id: String,
    #[serde(rename = "time")]
    pub event_time: DateTime<Utc>,
    #[serde(rename = "datacontenttype")]
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub data: String,
}

impl From<BusEvent> for CloudEvent {
    fn from(event: BusEvent) -> CloudEvent {
        let raw_data = serde_json::to_string(&event).unwrap();

        CloudEvent {
            cloud_events_version: "1.0".to_string(),
            event_type: event.event_type(),
            event_type_version: "0.1".to_string(),
            source: "https://wascc.dev/lattice/events".to_string(),
            subject: Some(event.subject()),
            event_id: Uuid::new_v4().to_hyphenated().to_string(),
            event_time: Utc::now(),
            content_type: "application/json".to_string(),
            data: raw_data,
        }
    }
}
