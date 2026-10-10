# Ubuntu services

These units are prepared templates; they were not installed or run under systemd in the cloud container.

## Save and Start UI service

For browser controls, prepare the user, installation, chrony, writable data directory, and `/etc/apexmind/config.toml` described below. Copy `deploy/apex-ui.service` to `/etc/systemd/system/`, then run `systemctl daemon-reload` and `systemctl enable --now apex-ui`. Its loopback-only page is on port 8765. Access it through the SSH tunnel in the main README; account/key information is entered on that page and stored under `/var/lib/apexmind/ui`.

The service expects qualified strategy artifacts under `/var/lib/apexmind/evaluation`, measured latency in `/var/lib/apexmind/latency.json`, and the laboratory registry in `/var/lib/apexmind/laboratory/registry.json`. Override `APEX_REPORT`, `APEX_MODEL`, `APEX_LATENCY`, `APEX_REGISTRY`, or `APEX_STAGE` in `/etc/apexmind/ui.env` if needed. Keep signing-key values out of that file. Native calibration and research must still be completed using the main README before Start can authorize trading.

Use either `apex-ui` to own the live runner or `apex-live`, with one controller per account. The UI shows Stopping while the runner exits and owned recovery runs; failed recovery remains blocked. `systemctl stop apex-ui` invokes the same cleanup. A UI restart restores saved API info and requires clicking Start again. Browser tabs and SSH tunnels can close while the service continues running. The new UI unit is statically checked only; actual boot, native account access, and recovery on your host remain to be verified.

## Direct live service

1. Install Python 3.12, uv, and chrony. Place the reviewed repository at `/opt/apexmind`, run `uv sync --frozen --all-extras`, and create an unprivileged `apexmind` user/group. Create `/var/lib/apexmind`, owned by that account. The application/virtual environment must be readable/executable by the service user.
2. Copy `config/default.toml` to `/etc/apexmind/config.toml`. Set `storage.database = "/var/lib/apexmind/apex.sqlite"`, `storage.artifacts = "/var/lib/apexmind/artifacts"`, and `live.ledger = "/var/lib/apexmind/live.sqlite"`.
3. Provision `/etc/apexmind/signing.key`, owned by `apexmind`, mode `0600`. Copy `config/live.env.example` to `/etc/apexmind/live.env` and fill account/API identifiers, paths, and stage. Keep the key value out of that environment file. Production is the guarded default; select pilot explicitly to gather qualifying forward evidence.
4. If needed, put supported proxy/trusted CA settings in `/etc/apexmind/network.env`. Keep TLS verification enabled and use the supported network route.
5. As the service user, verify chrony, `apex doctor --network`, `apex auth-check`, and `apex collect --seconds 30`, using `--config /etc/apexmind/config.toml`. Follow the calibration/research sequence in the main README. Native integration incurs actual costs. Credentials alone do not qualify deployment.
6. Set `APEX_REPORT`, `APEX_MODEL`, and `APEX_LATENCY` to the actual qualifying artifacts. Set `APEX_REGISTRY` to the laboratory registry for continuous adoption. Verify the intended stage with `live-check --account`.
7. Review/copy units to `/etc/systemd/system/`, run `systemctl daemon-reload`, and enable/start the desired services. For live operation: `systemctl enable --now apex-live`. Inspect `journalctl -u apex-live` and `apex live-status`.

The live unit recovers owned exposure before startup and after exit, handles SIGTERM, bounds failed restarts, and retains native protection during uncertainty. The live runner captures public data itself; a separate collector is optional while live. Collector/lab units support research before deployment. Complete one successful finite lab cycle before enabling `apex-lab`. Its registry is `/var/lib/apexmind/laboratory/registry.json`; set the live registry to that path for adoption. Do not use multiple ledgers/runners to control the same account.

Stop with `systemctl stop apex-live`, then verify recovery and actual account state. An unknown transaction may prevent verified cleanup. Preserve the ledger and inspect the exchange; never delete state or blindly repeat an order. `recover` refuses unmanaged exposure. Native conditional orders remain until flatness is verified.

After suspension, reconcile to flat and run `apex resume --report ... --model ... --latency ... --stage ...`. Resume verifies evidence/account state; unsupported recent performance requires a new qualified model. It is not a gate bypass.

Back up SQLite using its backup API or after clean shutdown, including active WAL. Monitor disk space, feed continuity, account age, clock bound, actual cash episodes, and native protection. Logs/artifacts need retention. Service boot behavior and actual native execution require verification on your Ubuntu host.
