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

use std::{path::PathBuf, time::Duration};
use structopt::clap::AppSettings;
use structopt::StructOpt;

extern crate latticeclient;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    global_settings(&[AppSettings::ColoredHelp, AppSettings::VersionlessSubcommands, AppSettings::GlobalVersion]),
    name = "latticectl",
    about = "A command line utility for interacting with a waSCC lattice")]
/// latticectl interacts with a waSCC lattice the same way the waSCC host would, and uses the same
/// environment variables to connect by default
struct Cli {
    #[structopt(flatten)]
    command: CliCommand,

    /// The host IP of the nearest NATS server/leaf node to connect to the lattice
    #[structopt(
        short,
        long,
        env = "LATTICE_HOST",
        hide_env_values = true,
        default_value = "127.0.0.1"
    )]
    url: String,

    /// Credentials file used to authenticate against NATS
    #[structopt(
        short,
        long,
        env = "LATTICE_CREDS_FILE",
        hide_env_values = true,
        parse(from_os_str)
    )]
    creds: Option<PathBuf>,

    /// Lattice invocation / request timeout period, in milliseconds
    #[structopt(
        short = "t",
        long = "timeout",
        env = "LATTICE_RPC_TIMEOUT_MILLIS",
        hide_env_values = true,
        default_value = "500"
    )]
    call_timeout: u64,
}

#[derive(Debug, Clone, StructOpt)]
enum CliCommand {
    /// List entities of various types within the lattice
    #[structopt(name = "list")]
    List {
        /// The entity type to list (actors, bindings, capabilities(caps), hosts)
        entity_type: String,
    },
}

fn main() {
    let args = Cli::from_args();
    let cmd = args.command;

    std::process::exit(
        match handle_command(
            cmd,
            args.url,
            args.creds,
            Duration::from_millis(args.call_timeout),
        ) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("Latticectl Error: {}", e);
                1
            }
        },
    )
}

fn handle_command(
    cmd: CliCommand,
    url: String,
    creds: Option<PathBuf>,
    timeout: Duration,
) -> Result<(), Box<dyn ::std::error::Error>> {
    match cmd {
        CliCommand::List { entity_type } => list_entities(&entity_type, &url, creds, timeout),
    }
}

fn list_entities(
    entity_type: &str,
    url: &str,
    creds: Option<PathBuf>,
    timeout: Duration,
) -> Result<(), Box<dyn ::std::error::Error>> {
    let client = latticeclient::Client::new(url, creds, timeout);
    match entity_type.to_lowercase().trim() {
        "hosts" => render_hosts(&client),
        "actors" => render_actors(&client),
        "bindings" => render_bindings(&client),
        "capabilities" | "caps" => render_capabilities(&client),
        _ => Err(
            "Unknown entity type. Valid types are: hosts, actors, capabilities, bindings".into(),
        ),
    }
}

fn render_actors(client: &latticeclient::Client) -> Result<(), Box<dyn ::std::error::Error>> {
    let actors = client.get_actors()?;
    for (host, actors) in actors {
        println!("\nHost {}:", host);
        for actor in actors {
            let md = actor.metadata.clone().unwrap();
            println!(
                "\t{} - {}  v{} ({})",
                actor.subject,
                actor.name(),
                md.ver.unwrap_or("???".into()),
                md.rev.unwrap_or(0)
            );
        }
    }
    Ok(())
}

fn render_hosts(client: &latticeclient::Client) -> Result<(), Box<dyn ::std::error::Error>> {
    let hosts = client.get_hosts()?;
    for host in hosts {
        println!("{}", host);
    }
    Ok(())
}

fn render_capabilities(client: &latticeclient::Client) -> Result<(), Box<dyn ::std::error::Error>> {
    let caps = client.get_capabilities()?;
    for (host, caps) in caps {
        println!("{}", host);
        for cap in caps {
            println!(
                "\t{},{} - Total Operations {}",
                cap.descriptor.id,
                cap.binding_name,
                cap.descriptor.supported_operations.len()
            );
        }
    }
    Ok(())
}

fn render_bindings(client: &latticeclient::Client) -> Result<(), Box<dyn ::std::error::Error>> {
    let bindings = client.get_bindings()?;
    for (host, bindings) in bindings {
        println!("Host {}", host);
        for binding in bindings {
            println!(
                "\t{} -> {},{} - {} values",
                binding.actor,
                binding.capability_id,
                binding.binding_name,
                binding.configuration.len()
            );
        }
    }
    Ok(())
}
