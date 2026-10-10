# APEX MIND 0.3.0 validation

The UI extension was validated on 2026-10-10. Version 0.2.0 network/account observations below are retained from the earlier cloud checks; this extension does not establish new native exchange readiness.

| Check | Observed result |
|---|---|
| Frozen installation with all extras | Passed; Python 3.12.14, version 0.3.0, pinned SDK 1.1.6 |
| Ruff | Passed |
| Automated offline tests | **79 passed**, with two SDK deprecation warnings |
| Local HTTP credential/control tests | Passed; same-origin/token/host restrictions, private storage, no key returned |
| Manager process lifecycle | Passed with real child processes and scripted native commands; single Start, SIGTERM, recovery; no real orders |
| Chromium desktop UI | Passed; three fields, Save clears the key, status contains no key, missing qualification blocks Start |
| Chromium mobile UI | Passed at width 360; no horizontal overflow |
| UI server restart | Passed; identifiers and saved-key marker restored, key remains blank, no automatic trading |
| Native SDK shared-library loading | Passed; no real key generated or real transaction signed |
| UI systemd static verification | Passed after mapping executable/data paths to the checkout; unit not installed or started |
| Source distribution / wheel | Built and checked for included UI assets; no captured data, reports, or private keys |
| Lighter + Kraken REST/catalog mapping (0.2.0 check) | Passed; exact BTC/ETH mappings |
| Kraken streaming (0.2.0 check) | Passed; BTC/ETH books and trades |
| Lighter public WebSocket (0.2.0 check) | Failed: handshake HTTP **400** on the provided route |
| Full finite public collector (0.2.0 check) | Failed required-channel coverage because Lighter channels did not arrive |
| Public account transaction-history probe (0.2.0 check) | Blocked: HTTP **403**; authenticated endpoint semantics remain unverified |
| Read-only authenticated account check | Unrun: actual authentication absent |
| Host NTP error bound | Blocked: chrony tracking unavailable |
| Actual native lifecycle / margin execution | Unrun: account, clock, and native stream prerequisites missing |
| Actual trading advantage | Unproven; no real strategy executions |

The UI uses protected local credential files. Browser automation used nonfunctional test API material in /tmp, not actual account authentication. No real account transaction was submitted. The UI binds to loopback, requires its per-process token and a matching origin for actions, and provides no saved-key retrieval endpoint. It manages one runner, checks existing ledger ownership before changing API info, and preserves existing evidence/native protection requirements.

The latest synthetic end-to-end evaluation is `reports/ui-0.3-release-validation/evaluation/report.json` and `report.md`. It records 2,336 tested sides, 2,304 labels, 18 model/fold comparisons, and zero selected trades. Source/model hashes match the current implementation. Synthetic market data and latency never qualify live deployment. Both profitability and live-deployment flags remain false. Older validation directories are historical and may refer to prior source hashes.

Source manifest SHA256: `ce3c825013282cb57352fbd6af5c422d95e1404590fc21bbd1d02ca62e1d0fb5`.

Offline lifecycle tests cover durable pre-submit journals, acknowledged versus executed orders, uncertain POST outcomes without retries, final preflight expiry, partial/canceled fills, duplicate reconciliation, native position-tied stops, parent identity, clipped reduce-only quantities, conservative price rounding, ownership mismatches, stale/missing native effects, margin/drawdown guards, actual flat-to-flat cash, external deposits, locked writers/account binding, audit checkpoints, pilot versus production evidence, and failure preserving an incumbent. These tests do not certify actual exchange behavior.

The earlier finite public capture recorded 10,521 BTC and 5,365 ETH reference books, plus 53 BTC and 35 ETH trades. No Lighter books/trades/funding arrived in that run. Public P&L checks accepted hourly resolution and rejected minute resolution. SDK examples confirmed millisecond explicit expiry and zero-size position-tied children. Native qualification must still verify those semantics on the authorized host.

Provision actual authentication securely on your host, verify chrony and native Lighter channels, then follow README calibration/research/pilot/production instructions. Start reports missing prerequisites; saving API info does not manufacture qualification. Default sizing remains conservative and cannot trade BTC/ETH at a $10 balance under their current $10 quote minimums.

Source/service files remain local workspace changes, not pushed to GitHub. Cloud setup/start drafts are saved separately; saving does not publish, start services, or place trades. Actual service boot and fresh-task restoration remain unverified. Development and local UI capabilities are established; full live deployment readiness is not.
