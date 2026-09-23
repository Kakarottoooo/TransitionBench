"""Versioned evidence export and bounded, path-safe import."""
import hashlib
import html
import json
import shutil
import tempfile
import zipfile
from pathlib import Path
from .metrics import summarize
from .schemas import RunManifest, RequestEvent
from .verifier import MAX_BYTES, REQUIRED, verify_bundle


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)


def write_json(path, value):
    Path(path).write_text(json_text(value), encoding="utf-8")


def append_jsonl(path, value):
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
        stream.flush()


def export_bundle(directory, manifest: RunManifest, requests: list[RequestEvent], transitions, decisions, validity=None):
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=False)
    summary = summarize(requests, manifest.experiment.slo, manifest.experiment.observation_s)
    summary.update(mode=manifest.mode, origin=manifest.origin, run_id=manifest.run_id)
    write_json(root / "manifest.json", manifest.model_dump(mode="json"))
    write_json(root / "summary.json", summary)
    for filename, rows in [("requests", requests), ("transitions", transitions), ("decisions", decisions)]:
        (root / (filename + ".jsonl")).touch()
        for row in rows:
            append_jsonl(root / (filename + ".jsonl"), row.model_dump(mode="json") if hasattr(row, "model_dump") else row)
    write_json(root / "resources.json", manifest.resource_intervals)
    write_json(root / "quality.json", {"gate": "nonempty + finish_reason stop + task check where available",
               "valid": sum(r.quality_valid for r in requests), "offered": len(requests),
               "assessment": "Synthetic/test-server checks do not establish model task quality"})
    write_json(root / "validity.json", validity or {"valid": True, "errors": []})
    report = render_report(manifest.model_dump(mode="json"), summary, requests, decisions)
    (root / "report.html").write_text(report, encoding="utf-8")
    checksums = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.iterdir())}
    write_json(root / "checksums.json", checksums)
    return summary


def render_report(manifest, summary, requests, decisions):
    # All imported values are escaped; this report has no scripts or remote assets.
    esc = lambda value: html.escape(str(value))
    curve = summary["cumulative"]
    horizon, count = summary["observation_s"], max(1, summary["offered"])
    points = "0,200 " + " ".join(f'{p["at_s"] / horizon * 900:.3f},{200 - p["qualified"] / count * 190:.3f}' for p in curve)
    rows = "".join(f'<tr><td>{esc(k)}</td><td>{v["offered"]}</td><td>{v["qualified"]}</td><td>{v["attainment"]:.1%}</td></tr>' for k, v in summary["by_class"].items())
    return f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:">
<title>TransitionBench evidence — {esc(manifest['run_id'])}</title><style>
body{{font:16px/1.6 system-ui,sans-serif;color:#172b42;background:#edf2f6;margin:auto;max-width:1000px;padding:40px}}
h1{{font-size:42px;line-height:1.1}}section{{background:white;border:1px solid #cbd7e0;border-radius:12px;padding:24px;margin:20px 0}}
pre{{overflow:auto;white-space:pre-wrap;font-size:12px}}td,th{{text-align:left;padding:10px 25px 10px 0}}svg{{width:100%;height:auto}}
.badge{{color:#674319;background:#ffecd0;padding:8px 12px;display:inline-block;border-radius:6px}}strong{{font-size:24px}}</style>
<p>TRANSITIONBENCH / EVIDENCE NOTEBOOK</p><h1>Should we deploy the faster configuration now?</h1>
<p class="badge">{esc(manifest['mode'])} · {esc(manifest['origin'])}</p>
<p>Run {esc(manifest['run_id'])}. Integrity checks are not independent certification.</p>
<section><strong>{summary['qualified']} / {summary['offered']}</strong> offered requests qualified · {summary['goodput_rps']:.3f} requests/s
<p>Common observation: {horizon:g} seconds. Scheduled-arrival SLO: {manifest['experiment']['slo']['e2e_s']:g}s end-to-end.</p>
<svg viewBox="0 0 900 220" role="img" aria-label="Cumulative qualifying completions"><path d="M0 200H900" stroke="#aabac8"/>
<polyline points="{points}" fill="none" stroke="#126e82" stroke-width="3"/></svg>
<table><tr><th>Class</th><th>Offered</th><th>Qualified</th><th>Attainment</th></tr>{rows}</table></section>
<section><h2>Decision evidence</h2><pre>{esc(json_text([d.model_dump(mode="json") if hasattr(d,'model_dump') else d for d in decisions]))}</pre></section>
<section><h2>Definitions and limitations</h2><pre>{esc(json_text(summary['definitions']))}</pre><pre>{esc(json_text(manifest['limitations']))}</pre>
<p>{esc(manifest['omitted_sensitive_data'])}</p><p>Verify: <code>transitionbench verify PATH_TO_BUNDLE</code></p></section>
<section><h2>Raw event excerpt</h2><pre>{esc(json_text([r.model_dump(mode="json") for r in requests[:4]]))}</pre></section></html>'''


def zip_bundle(directory, output):
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(Path(directory).iterdir()):
            archive.write(path, path.name)


def import_bundle(archive_path, destination):
    if Path(archive_path).stat().st_size > MAX_BYTES:
        raise ValueError("Import exceeds 32 MiB")
    if not zipfile.is_zipfile(archive_path):
        raise ValueError("Import must be a valid TransitionBench evidence ZIP")
    with tempfile.TemporaryDirectory(dir=Path(destination).parent) as scratch:
        with zipfile.ZipFile(archive_path) as archive:
            entries = archive.infolist()
            if len(entries) != len(REQUIRED) + 1 or {e.filename for e in entries} != REQUIRED | {"checksums.json"}:
                raise ValueError("Unsafe, duplicate, or missing archive paths")
            if sum(e.file_size for e in entries) > MAX_BYTES or any(e.file_size > MAX_BYTES or ((e.external_attr >> 16) & 0o170000) == 0o120000 for e in entries):
                raise ValueError("Oversized or linked archive entry")
            for entry in entries:
                if entry.flag_bits & 1:
                    raise ValueError("Encrypted evidence archives are unsupported")
                try:
                    data = archive.read(entry)
                except (zipfile.BadZipFile, RuntimeError):
                    raise ValueError("Corrupt or unsupported archive contents") from None
                if len(data) != entry.file_size:
                    raise ValueError("Archive size mismatch")
                (Path(scratch) / entry.filename).write_bytes(data)
        result = verify_bundle(scratch)
        if not result["integrity_valid"]:
            raise ValueError("Invalid evidence: " + "; ".join(result["integrity_errors"]))
        # Revalidate the canonical typed schema; the independent verifier stays standalone.
        RunManifest.model_validate_json((Path(scratch) / "manifest.json").read_text(encoding="utf-8"))
        for line in (Path(scratch) / "requests.jsonl").read_text(encoding="utf-8").splitlines():
            RequestEvent.model_validate_json(line)
        shutil.copytree(scratch, destination)
    return result
