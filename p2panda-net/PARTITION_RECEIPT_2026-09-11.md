# Rejoining a shared overlay

Turnstone's original `a_partition_heals_and_both_sides_converge` failed after
the host dropped and recreated its lanes. A one-way invitation gave the guest
the host's address, but inbound connections did not give the host a dial hint.
Its repeated outbound sync attempts failed with `TransportInfoMissing` before
a sync round began.

Completed inbound handshakes now contribute their actual open transport paths
to an endpoint-local cache. Address-book records take precedence. These hints
are never promoted to signed discovery records. `IrohConfig` exposes cache
capacity (default 4096) and lifetime (default 30 minutes); zero capacity disables
it. Relay and custom paths retain iroh's transport addresses.

A separate shared-overlay gap is also repaired: new sync managers subscribe to
future gossip events and obtain current neighbours in the same actor turn.
The regression verifies a late manager receives a specific missed operation,
then verifies immediate recreation under the same ALPN receives another.

Validation with Rust 1.97.1:

- Original Turnstone partition gate: baseline failed in 76.14 seconds; local
  patched dependency passed in 20.22 seconds. Timeout and convergence assertions
  are unchanged. Final immutable consumer receipt belongs in Turnstone.
- Endpoint one-way invitation and reverse connection: passed in 0.19 seconds,
  using OS-allocated ports and the actual host address.
- Late and recreated sync manager regression: passed in the serial library run.
- Broad library run: 39 passed, 5 failed, 1 ignored. The Windows bind failures are
  not a green receipt. A focused discovery run identified error 10013 on
  `127.0.0.1:62166`; Windows UDP exclusions include `62084..62183` for IPv4 and
  IPv6. No listener owned the tested ports. No OS configuration was changed.

Detailed bind logging now retains the nested cause and requested addresses.
The other seeded-port tests remain subject to host reservations. Temporary
logs: `C:/t/turnstone-chat-observed-address.log`,
`C:/t/p2panda-net-final-local.log`, `C:/t/p2panda-bind-detail.log`.
