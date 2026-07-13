# Source Observations

- Date: 2026-07-13
- Status: Reproducible observations, not long-term benchmarks

## Official Sources

The API reference, API policy, Terms of Service, rules index, and SkyBlock rules
listed in `intake/SOURCE_MANIFEST.md` were retrieved on 2026-07-13. The API
policy page reported revision 2025-09-28. The SkyBlock rules article reported
revision 2024-05-06. These dynamic sources must be reviewed again before
collector activation and releases.

## Live Payload Samples

| Endpoint | SHA-256 of Retrieved Body | Observed Summary |
|---|---|---|
| `https://api.hypixel.net/v2/skyblock/bazaar` | `aa8e0ce1c3df9943219d8755c2081cb8631d1f4786752111aed5dda4d155b0a6` | 3,222,520 bytes; 1,933 products; 54,365 summary levels |
| `https://api.hypixel.net/v2/resources/skyblock/items` | `c018e6c45b3c4bc1f8bd874cf9e267930c25869f412a87c365d3f2fcffdbf1b0` | 5,524 unique item IDs; approximately half of Bazaar IDs directly matched |

The bodies were fetched through the session web-fetch tool and hashed with
`sha256sum`. They are not committed. A future reproduction must record a new
timestamp and hash because the endpoints are live and mutable.

## Deployment Host Preflight

Read-only SSH commands inspected `hostnamectl`, `lscpu`, `free -h`, `lsblk`,
`df -hT`, Docker/Compose versions, network interfaces, Docker service state, and
`timedatectl`. The committed result intentionally excludes hostname, address,
credentials, machine ID, boot ID, MAC addresses, and SSH material.

Observed capacity: Ubuntu 26.04 LTS, Ryzen 3 3200U with two cores/four threads,
5.2 GiB RAM, 8 GiB swap, Docker 29.1.3, Compose 2.40.3, synchronized UTC clock,
and about 870 GiB free on ext4 `/srv`.

These observations do not replace sustained ingestion, query, memory, or
backup/restore benchmarks.
