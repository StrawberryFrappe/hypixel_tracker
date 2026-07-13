# Source Manifest

Sources are ordered by authority within their subject. Official policy wins
over product preferences when they conflict.

| ID | Source | Authority and Use |
|---|---|---|
| SRC-001 | User interview, 2026-07-13 | Product scope, users, releases, deployment posture, non-goals, and quality priorities |
| SRC-002 | `https://api.hypixel.net/`, retrieved 2026-07-13 | Official Bazaar and item endpoint schema and field semantics |
| SRC-003 | `https://developer.hypixel.net/policies`, retrieved 2026-07-13; page revision 2025-09-28 | API use, caching, branding, no-proxy, and game-integrity constraints |
| SRC-004 | `https://hypixel.net/terms`, `https://hypixel.net/rules`, and `https://support.hypixel.net/hc/en-us/articles/4508088842898-Hypixel-SkyBlock-Rules`, retrieved 2026-07-13 | Terms, server rules, and SkyBlock rules; compliance constraints override desired features |
| SRC-005 | Live Bazaar response inspected 2026-07-13 | Observed payload size, product count, field ranges, and order-book shape |
| SRC-006 | Live SkyBlock item response inspected 2026-07-13 | Product metadata coverage and missing virtual-item cases |
| SRC-007 | `main` commit `c8ae84fc0a0b1376a16f310934a032e7c7b77669` | Current implementation state only |
| SRC-008 | `origin/lean` commit `028f05169e647cf9ee65ef98bd915b97b6a4a931` | Evidence and selective ideas only; not an implementation baseline |
| SRC-009 | Harness kernel `d7a31eacd3f595b24df9c8245f94796cbd177325` | Mounting structure and unconditional acceptance workflow |
| SRC-010 | Read-only host preflight, 2026-07-13 | Deployment CPU, RAM, storage, Docker, network, and time configuration |

## Observed Data Evidence

The inspected Bazaar response was about 3.22 MB and contained 1,933 products,
21,549 sell-summary levels, and 32,816 buy-summary levels. Full normalization
of every level at an approximately 20-second source cadence would create about
235 million offer rows per day. These are sample observations, not guaranteed
capacity constants.

The items resource contained 5,524 IDs but directly matched only about half of
the Bazaar products. Enchantment, shard, essence, and other virtual IDs require
deterministic fallback display metadata.

The Bazaar sample SHA-256 is
`aa8e0ce1c3df9943219d8755c2081cb8631d1f4786752111aed5dda4d155b0a6`.
The item sample SHA-256 is
`c018e6c45b3c4bc1f8bd874cf9e267930c25869f412a87c365d3f2fcffdbf1b0`.
Raw samples are intentionally not committed. Reproduction therefore requires a
new dated fetch and comparison rather than assuming these bodies remain current.

## Repository Authority

Neither existing branch defines the target architecture. User decisions and
official sources define requirements; reviewed ADRs define implementation
choices.
