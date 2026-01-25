from app.core.models import SyncConfig, RunResult
from app.services.update_checker import should_process_file
from app.services.excel_parser import parse_excel_to_delta
from app.services.data_quality import run_data_quality_checks

def run_sync(config: SyncConfig) -> RunResult:
    # Gate: should we process the file?
    gate = should_process_file(config)
    if not gate.get("should_process", False):
        return RunResult(status="skipped", gate=gate)

    # Parse: process file into delta
    parse_result = parse_excel_to_delta(config)
    if parse_result.get("status") != "success":
        return RunResult(status="error", gate=gate, parse=parse_result)

    # Data quality: run quality checks
    dq_result = run_data_quality_checks(config)
    if dq_result.get("checks_passed", False):
        status = "success"
    else:
        status = "dq_failed"

    return RunResult(status=status, gate=gate, parse=parse_result, dq=dq_result)
