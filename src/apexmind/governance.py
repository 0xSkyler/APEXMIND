import json
from pathlib import Path
import time
from .evidence import research_gate


def validate_evidence(report_path, model_path, integration_path=None):
    report = json.loads(Path(report_path).read_text())
    research = research_gate(report, model_path)
    reasons = list(research["reasons"])
    # An exported JSON document is reviewable but cannot prove current account
    # state. The operational live-check command uses NativeGateway directly.
    reasons.append(
        "authorized exchange lifecycle and margin integration evidence missing"
        if not integration_path
        else "exported integration evidence must be reverified against the account"
    )
    return {
        "allowed": False,
        "research_allowed": research["allowed"],
        "reasons": list(dict.fromkeys(reasons)),
    }


def register_challenger(report_path, model_path, registry):
    report = json.loads(Path(report_path).read_text())
    Path(registry).parent.mkdir(parents=True, exist_ok=True)
    old = json.loads(Path(registry).read_text()) if Path(registry).exists() else {}
    candidate = {
        "report": str(Path(report_path).resolve()),
        "model": str(Path(model_path).resolve()),
        "sha256": report["model_sha256"],
        "created_ns": time.time_ns(),
        "champion": report["champion"],
        "validation": validate_evidence(report_path, model_path),
    }
    old["challenger"] = candidate
    # Promotion is fail-closed; synthetic or assumption-based evidence cannot
    # replace an approved incumbent. Preserve the incumbent on every failure.
    if candidate["validation"]["allowed"]:
        old["champion"] = candidate
    temporary = Path(registry).with_suffix(".tmp")
    temporary.write_text(json.dumps(old, indent=2))
    temporary.replace(registry)
    return candidate
