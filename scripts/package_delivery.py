"""Archive owned deliverables and evidence, excluding environments and local state."""
import hashlib
import json
import zipfile
import argparse
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--reviewer', action='store_true', help='Package runnable source, evidence summaries and a GPU sample; full GPU archive is separate')
options = parser.parse_args()
VERSION = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']['version']
DESTINATION = ROOT.parent / (f"TransitionBench-reviewer-kit-v{VERSION}.zip" if options.reviewer else "TransitionBench-delivery.zip")
EXCLUDED = {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache", "work", ".transitionbench", "test-results"}
EXPLORATORY = {'cpu-study','cpu-study-v2','final-study','local-http-final','local-http-v3','strong-baseline-study'}


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def included(path):
    relative = path.relative_to(ROOT)
    if options.reviewer:
        if relative.as_posix() == 'reports/package-verification-v031.json':
            return False  # Describes the final ZIP; keep it outside its own archive.
        # Keep the five-minute review small. The separately delivered final GPU
        # archive retains full campaigns, journals, failures and host evidence.
        if relative.parts[:2] == ('reports', 'vast-51687766'):
            # Raw campaigns are separate immutable downloads. Never recursively
            # collect multi-GB journals/exports into the quick review package.
            allowed = (len(relative.parts) == 3 and path.suffix in {'.json', '.md', '.xml'}
                       or len(relative.parts) == 4 and relative.parts[2] == 'formal-v7'
                       and path.name in {'independent-verification.json', 'results-summary.json',
                                         'frozen-study.json', 'supervisor-state.json', 'export-receipt.json'})
            if not allowed or path.stat().st_size > 2_000_000:
                return False
        if relative.parts[:2] == ('reports', 'sender-diagnosis'):
            if len(relative.parts) != 3 or path.suffix not in {'.md', '.json', '.xml'} or path.stat().st_size > 2_000_000:
                return False
        if len(relative.parts) > 2 and relative.parts[0] == 'reports' and relative.parts[1].startswith('capacity-screen-'):
            if len(relative.parts) != 3 or path.suffix not in {'.json', '.md', '.xml'} or path.stat().st_size > 2_000_000:
                return False
        if relative.parts[0]=='reports' and (relative.parts[1] in EXPLORATORY or path.name=='delivery-files.json'):
            return False
        if relative.parts[0]=='dist' and path.name not in {
                f'transitionbench-{VERSION}-py3-none-any.whl', f'transitionbench-{VERSION}.tar.gz',
                'transitionbench-local-client-0.1.0.tgz'}:
                return False
        if relative.parts[:3]==('reports','collection-rehearsal','state'):
            return False
    return (not any(part in EXCLUDED for part in relative.parts)
            and not path.name.endswith((".db", ".db-wal", ".db-shm", ".pyc"))
            and not path.name.startswith('.env') and path.suffix not in {'.pem','.key'}
            and not (relative.parts[0] in {"web", "sdk"} and "dist" in relative.parts))


files = sorted(path for path in ROOT.rglob("*") if path.is_file() and included(path))
manifest_path = ROOT / "reports" / ("reviewer-files.json" if options.reviewer else "delivery-files.json")
manifest = {"schema_version": "1.0", "purpose": "Integrity inventory, not an authenticity certificate",
            "files": {path.relative_to(ROOT).as_posix(): {"sha256": sha256(path), "bytes": path.stat().st_size}
                      for path in files if path != manifest_path}}
manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
files = sorted(set(files) | {manifest_path})
with zipfile.ZipFile(DESTINATION, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
    for path in files:
        archive.write(path, "transitionbench/" + path.relative_to(ROOT).as_posix())
with zipfile.ZipFile(DESTINATION) as archive:
    assert archive.testzip() is None
checksum = sha256(DESTINATION)
DESTINATION.with_suffix(".zip.sha256").write_text(checksum + "  " + DESTINATION.name + "\n", encoding="utf-8")
print(json.dumps({"archive": str(DESTINATION), "files": len(files), "bytes": DESTINATION.stat().st_size, "sha256": checksum}, indent=2))
