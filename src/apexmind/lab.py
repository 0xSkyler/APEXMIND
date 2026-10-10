import asyncio
import logging
import os
from pathlib import Path
import shutil
import sys
import time

LOG = logging.getLogger(__name__)


async def laboratory(config_path, latency_path, out, interval=3600, cycles=None):
    if interval < 60:
        raise ValueError("Laboratory interval must be at least 60 seconds")
    if cycles is not None and cycles <= 0:
        raise ValueError("Cycle count must be positive")
    count, successful = 0, 0
    while cycles is None or count < cycles:
        if shutil.disk_usage(out if Path(out).exists() else ".").free < 500 * 1024**2:
            raise RuntimeError("Insufficient disk space for research artifacts")
        run = Path(out) / str(time.time_ns())
        run.mkdir(parents=True)
        env = os.environ.copy()
        env.update({"OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"})
        commands = [
            [
                sys.executable,
                "-m",
                "apexmind",
                "--config",
                config_path,
                "dataset",
                "--latency",
                latency_path,
                "--out",
                str(run / "dataset.npz"),
            ],
            [
                sys.executable,
                "-m",
                "apexmind",
                "--config",
                config_path,
                "research",
                "--dataset",
                str(run / "dataset.npz"),
                "--out",
                str(run / "evaluation"),
            ],
            [
                sys.executable,
                "-m",
                "apexmind",
                "--config",
                config_path,
                "register",
                "--report",
                str(run / "evaluation/report.json"),
                "--model",
                str(run / "evaluation/champion.joblib"),
                "--registry",
                str(Path(out) / "registry.json"),
            ],
        ]
        failed = False
        for index, command in enumerate(commands):
            with (run / f"step-{index}.log").open("w") as log:
                process = await asyncio.create_subprocess_exec(
                    *command, stdout=log, stderr=log, env=env
                )
                try:
                    code = await asyncio.wait_for(process.wait(), timeout=1800)
                except TimeoutError:
                    process.kill()
                    await process.wait()
                    code = 124
            if code:
                failed = True
                LOG.warning(
                    "Research cycle failed at step %s (exit %s); incumbent retained", index, code
                )
                break
        if not failed:
            successful += 1
        count += 1
        if cycles is None or count < cycles:
            # Async sleep keeps cancellation and other services responsive.
            await asyncio.sleep(interval)
    if cycles is not None and successful < count:
        raise RuntimeError(
            f"{count - successful} of {count} research cycles failed; inspect cycle logs"
        )
    return {
        "attempted_cycles": count,
        "successful_cycles": successful,
        "registry": str(Path(out) / "registry.json"),
    }
