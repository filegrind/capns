//! Bifaci — Binary Frame Cap Invocation protocol
//!
//! Three-layer architecture:
//! - **Router** (`relay_switch`): (RelaySwitch + RelayMaster × N)
//! - **Host × N** (`host_runtime`, `relay`): (RelaySlave + PluginHostRuntime)
//! - **Plugin × N** (`plugin_runtime`): (PluginRuntime + handler × N)

pub mod frame;
pub mod io;
pub mod manifest;
pub mod router;
pub mod plugin_runtime;
pub mod host_runtime;
pub mod relay;
pub mod relay_switch;
pub mod in_process_host;
pub mod plugin_repo;

#[cfg(test)]
mod integration_tests;
