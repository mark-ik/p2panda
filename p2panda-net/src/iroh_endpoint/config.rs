// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IrohConfig {
    /// IPv4 address to bind to.
    pub bind_ip_v4: Ipv4Addr,

    /// Port used for IPv4 socket address.
    ///
    /// Setting the port to `0` will use a random port. If the port specified is already in use, it
    /// will fallback to choosing a random port.
    pub bind_port_v4: u16,

    /// IPv6 address to bind to.
    pub bind_ip_v6: Ipv6Addr,

    /// Port used for IPv6 socket address.
    ///
    /// Setting the port to `0` will use a random port. If the port specified is already in use, it
    /// will fallback to choosing a random port.
    pub bind_port_v6: u16,

    /// Maximum authenticated inbound dial hints retained locally. Zero disables caching.
    pub observed_address_capacity: usize,

    /// How long an observed inbound path remains eligible as a fallback dial hint.
    pub observed_address_ttl: Duration,
}

impl Default for IrohConfig {
    fn default() -> Self {
        Self {
            bind_ip_v4: Ipv4Addr::UNSPECIFIED,
            bind_port_v4: 0,
            bind_ip_v6: Ipv6Addr::UNSPECIFIED,
            bind_port_v6: 0,
            observed_address_capacity: 4096,
            observed_address_ttl: Duration::from_secs(30 * 60),
        }
    }
}
