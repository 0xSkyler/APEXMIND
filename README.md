# APEX MIND

An execution-aware Lighter perpetual trading system with CVIFA research, forward paper testing, native execution, durable recovery, margin protection, and a bounded research laboratory. Python 3.12 on Ubuntu. The original specification is preserved in [docs/DIRECTIVE.md](docs/DIRECTIVE.md).

**Live execution code is implemented. Actual account integration and a profitable strategy have not been established in this cloud environment.** Synthetic validation is software evidence only. You supply authentication securely and run native qualification on your host.

## Simple local UI

```bash
.venv/bin/apex ui
```

Open `http://127.0.0.1:8765` in your browser. The page has just **Account index**, **API key index**, **API private key**, **Save**, and **Start**. It shows current status and a Stop control while the runner is active. Save writes the key to a protected local file and clears it from the form; it never sends a saved key back to the browser. Leave the key blank to keep it. Changing account/key index requires entering the corresponding key again.

Start checks real strategy/latency evidence, clock synchronization, account access, owned recovery, and the selected deployment stage before launching live trading. Missing prerequisites appear as a blocked status. It does not generate evidence or run cost-incurring calibration automatically. Stop signals the runner, then verifies owned exposure recovery; incomplete recovery remains visible. Closing a browser tab does not stop the runner. Stopping the UI server stops its managed runner and attempts recovery. After restarting the server, credentials persist and Start must be clicked again.

The UI uses existing research artifacts by default: `reports/cvifa-001/report.json`, `reports/cvifa-001/champion.joblib`, and `data/latency.json`. An operator can set `APEX_CONFIG`, `APEX_REPORT`, `APEX_MODEL`, `APEX_LATENCY`, `APEX_REGISTRY`, and `APEX_STAGE` before starting the UI, or pass its CLI options. Production remains the default; use `--stage pilot` explicitly after research and native qualification. These settings stay out of the form.

UI credentials default to `secrets/ui`, with directory mode `0700` and file mode `0600`. The UI binds only to loopback and rejects foreign hosts/origins. For a remote Ubuntu host, use an SSH tunnel:

```bash
ssh -N -L 8765:127.0.0.1:8765 your-user@your-host
```

Then open the same local address in your browser. [deploy/README.md](deploy/README.md) describes the UI service. Run one trading controller per account; use either the UI-managed runner or the existing live service.

## Install and verify

```bash
export UV_CACHE_DIR=/workspace/.cache/uv
uv sync --frozen --all-extras
.venv/bin/ruff check src tests
.venv/bin/pytest -q
.venv/bin/apex doctor --network
.venv/bin/apex demo --out reports/demo-001
```

Use a new demo directory. `uv.lock` pins dependencies, including optional `lighter-sdk==1.1.6`. Numerical work is capped at two threads. Tests cover a scripted HTTPS/signing round-trip, partial/canceled fills, uncertain submissions, persistent IDs, ownership checks, protection, cash accounting, promotion gates, and restart recovery. The scripted exchange is a fixture, not real trading history.

## Authentication and native qualification

Use a dedicated **classic USDC perpetual account**, cross margin, and one runner. Unmanaged positions/orders block trading. The application exposes trading/cancellation operations and no withdrawal or transfer interface.

1. Provision your API signing key in a regular local file, owned by the application's user, mode `0600`. Keep the key out of Git, TOML, logs, and chat. The reader checks the opened inode and rejects symlinks and proxy-secret placeholders.
2. Set `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`, and `LIGHTER_SIGNING_KEY_FILE`. The last value is the protected file's absolute path. [config/live.env.example](config/live.env.example) contains identifiers/path placeholders and service settings; it contains no key value.
3. Install/synchronize chrony. `chronyc tracking` must report normal synchronization and a measured offset/dispersion/delay bound within `[live].max_clock_error_ms`. Local clock stability alone is insufficient.
4. Check read-only authentication and native public channels:

```bash
.venv/bin/apex auth-check
.venv/bin/apex discover
.venv/bin/apex collect --seconds 30
```

Finite capture requires Lighter/reference books and trades plus Lighter funding for every selected symbol. Kraken linear perpetuals are the default reference. `symbols = ["ALL"]` discovers exact eligible overlaps; live entries also require market-specific research coverage. Unsupported inverse, spot, and multiplied contracts are excluded. Binance futures is optional through its REST/stream configuration.

**This next command submits native orders and incurs actual costs on the configured network.** It runs a protected post-only cancellation test, a triggered stop test, and bounded IOC round-trips in both directions. It refuses unsuitable balances, clock/feed failures, missing fills/history, and uncertain transactions.

```bash
.venv/bin/apex integration-test --symbol BTC --notional 12 --roundtrips 20 --latency-out data/latency.json
```

Choose a feasible symbol/notional from discovery. Exchange minimums, tick rounding, margin reserve, and risk sizing still apply. `--roundtrips 1` can diagnose the lifecycle but cannot supply 20 observations per entry/exit role. Full qualification persists native transaction identities, fills, settled all-in costs, cancellation/protection evidence, and submit-to-first-observed-fill latency. Latency includes polling/queue uncertainty; acknowledgements never count as fills. The margin test combines account-derived refusal scenarios with actual native stop/reduce-only fills; it does not deliberately approach liquidation.

Optional testnet qualification uses a separate TOML with `https://testnet.zklighter.elliot.ai` and `wss://testnet.zklighter.elliot.ai/stream`, separate credentials, database, and ledger. Testnet cannot authorize a mainnet ledger. Never change a ledger's account/key/network binding.

## Research and forward deployment

After calibration, collect new market data and evaluate it:

```bash
.venv/bin/apex collect
# After a sufficient chronological sample, in another terminal:
.venv/bin/apex dataset --latency data/latency.json --out artifacts/cvifa.npz
.venv/bin/apex research --dataset artifacts/cvifa.npz --out reports/cvifa-001
.venv/bin/apex register --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --registry artifacts/registry.json
```

Real datasets automatically exclude calibration-period events and report the excluded count and exact retained period. Labels use empirical positive entry/exit delays, subsequent executable depth, fees, slippage reserves, and conservative funding exposure. USDC versus reference USD/USDT is an explicit unhedged basis. Actual funding events remain separate in the account ledger.

Six families share purged chronological periods: reference momentum, cross-venue lag, order flow, combined lag/flow, regularized models, and gradient boosting. Features are causal; feature selection occurs in training, while calibration and learned entry thresholds precede held-out observations. Complex families require statistical advantage over simpler alternatives. Holding forecasts use the configured research horizon and measured exit delay; multi-horizon optimization is not claimed. Current live execution uses native IOC orders; passive execution is rejected without measured conditional fill evidence.

JSON/Markdown reports contain periods, markets, hashes, dependencies, assumptions, tested/rejected opportunities, hypothetical fills, gross/net P&L, funding reserves, slippage, latency, profit factor, expectancy, Expected Shortfall, confidence intervals, drawdown scope, regimes, and baselines. Source changes require reproducing research and native qualification. Load only trusted local `.joblib` artifacts.

Research must demonstrate positive adjusted held-out expectancy and separate positive periods, with the loaded family independently supported. Native qualification, actual authorization, matching account-tier latency/fees, continuous timestamped books, and margin checks must also pass. Assumed research fees must cover observed all-in costs.

Forward paper testing is available before risking strategy capital:

```bash
.venv/bin/apex paper-stream --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json
```

Paper fills never count as actual advantage. To gather actual forward evidence after research/native qualification, explicitly select **pilot**:

```bash
.venv/bin/apex live-check --account --stage pilot --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json
.venv/bin/apex live --stage pilot --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json --registry artifacts/registry.json
```

Pilot uses the same sizing/protection/retirement rules and makes no profitability claim. **Production** additionally requires at least 20 reconciled strategy cash episodes across separate days with positive 99% block-bootstrap expectancy:

```bash
.venv/bin/apex live-check --account --stage production --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json
.venv/bin/apex promote --stage production --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json --registry artifacts/registry.json
.venv/bin/apex live --stage production --report reports/cvifa-001/report.json --model reports/cvifa-001/champion.joblib --latency data/latency.json --registry artifacts/registry.json
```

An exported integration JSON cannot authorize orders. Native commands verify current account state. The lab produces challengers; registry adoption occurs only while flat, after prior cash settles and the candidate's gate passes. Pilot can adopt qualified research challengers; production adopts verified champions. Failures retain the incumbent. Deteriorating actual net performance suspends and reduces exposure. Operational suspensions require a verified flat account and `apex resume`; an unsupported retired model cannot resume through a flag change.

## Operation and recovery

```bash
.venv/bin/apex live-status
.venv/bin/apex recover --timeout 120
.venv/bin/apex lab --latency data/latency.json --out reports/laboratory --cycles 1
# After the finite cycle succeeds:
.venv/bin/apex lab --latency data/latency.json --out reports/laboratory --interval 3600
```

`recover` cancels owned entries and submits only reduce-only exits for reconciled owned exposure. It operates independently of the reference venue and retains native stops until flatness is proven. Unknown submission outcomes require exchange reconciliation; the original transaction is never blindly repeated. IDs and nonce journals are durable before POST. One writer owns the ledger. Account, market, side, size, and trade identities are verified; pagination and cumulative fills must agree.

The watchdog protects maintenance margin, reserves, effective gross exposure, liquidation proximity, drawdown, feed outages, missing protection, and holding deadlines. Atomic OTO stops survive process outages. Stops/bounded IOC exits may fail during gaps or illiquidity; those outcomes remain recorded and reconciled before further reduction. Portfolio cash checkpoints pause entries and flatten exposure at bounded intervals or before native history capacity is exhausted. This preserves auditable episodes without a daily trade cap. Net P&L is actual flat-to-flat cash with transfer-inclusive baselines and contamination checks. Concurrent positions form portfolio episodes rather than invented per-position net profits. Native fee ticks stay raw; combined fee/funding debit is measured from gross fills versus settled cash. Polling cannot measure intra-poll drawdown extrema.

The starting balance policy is **$10**, without a higher deposit rule or daily trade cap. Actual equity determines exposure. Exchange minimums/rounding may make a market infeasible; leverage is never raised to overcome a minimum. Current BTC/ETH quote minimums are $10, so conservative default reserves/market sizing cannot trade them at a $10 balance. Research defaults to $12 of depth to allow tick rounding above those minima when equity permits. Allocation uses conservative downside/exposure budgeting, not a claimed optimal correlation optimizer.

The lab caps CPU, event count, and stage duration. Use a SQLite backup/retention policy; copying only the main database while WAL is active can lose data. [deploy/README.md](deploy/README.md) explains services that run without SSH, secure paths, startup recovery, and shutdown handling. `deploy/run-live.sh` launches locally after exporting the documented configuration.

## Cloud validation

Lighter/Kraken REST and Kraken streaming were verified. Lighter WSS failed its native handshake through this environment's supplied proxy. Chrony and account authentication are absent. No real account transactions/trades were submitted. Synthetic evaluation selected no trades and established no alpha. Exact checks and remaining prerequisites are in [docs/VALIDATION.md](docs/VALIDATION.md).

Source is retained in this workspace and has not been pushed to GitHub. Review/publish the cloud environment to retain the prepared filesystem. A fresh clone of the originally empty remote needs the implementation committed/pushed first. Processes do not survive snapshot restoration.
