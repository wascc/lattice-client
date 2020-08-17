[![crates.io](https://img.shields.io/crates/v/latticeclient.svg)](https://crates.io/crates/latticeclient)&nbsp;
![Rust build](https://github.com/wascc/lattice-client/workflows/Rust/badge.svg)&nbsp;
![license](https://img.shields.io/crates/l/latticeclient.svg)&nbsp;
[![documentation](https://docs.rs/latticeclient/badge.svg)](https://docs.rs/latticeclient)


# Lattice Client

This library, and its accompanying binary, `latticectl` (pronunciation determined by the reader) supports interaction with a waSCC lattice by connecting to one of the NATS servers in the lattice. All waSCC hosts compiled with the _lattice_ feature enabled can automatically form and join self-healing, self-maintaining, infrastructure-agnostic clusters called **lattices**.

For more information about what you can do with the lattice and the different types of supported interactions, check out the Rust documentation for this library and lattice documentation at [wascc.dev](https://wascc.dev/docs/lattice/overview/).

This is very, very early stuff and the interaction patterns and message contracts for the lattice are almost surely going to change rapidly, so please keep that in mind as you experiment.
