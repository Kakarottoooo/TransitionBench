import json
import subprocess
import sys
import zipfile
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from transitionbench.api import create_app
from transitionbench.evidence import export_bundle,import_bundle,zip_bundle
from transitionbench.schemas import RunManifest,ExperimentSpec,RequestEvent


def test_cli_api_error_is_json_not_traceback(asgi_server,tmp_path):
    base=asgi_server(create_app(tmp_path/'api'))
    file=tmp_path/'bad.json';file.write_text('{"mode":"not-a-mode"}')
    result=subprocess.run([sys.executable,'-m','transitionbench.cli','--api',base,'run',str(file)],capture_output=True,text=True)
    assert result.returncode==1
    assert json.loads(result.stdout)['error']['code']=='api_error'
    assert 'Traceback' not in result.stderr


def test_invalid_zip_returns_structured_error(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        response=client.post('/api/v1/bundles/import',files={'file':('bad.zip',b'not a zip','application/zip')},headers={'X-TransitionBench':'1'})
        assert response.status_code==422
        assert response.json()['error']['code']=='invalid_operation'


def test_html_report_escapes_imported_text_and_text_is_not_exported(tmp_path):
    manifest=RunManifest(run_id='test',mode='SIMULATION',origin='synthetic',experiment=ExperimentSpec(),offered_ids=['a'],created_at_unix_s=0,versions={},limitations=['</pre><img src=x onerror=alert(1)>'])
    bundle=tmp_path/'bundle'
    export_bundle(bundle,manifest,[RequestEvent(request_id='a',scheduled_s=0)],[],[])
    report=(bundle/'report.html').read_text(encoding='utf-8')
    assert '<img src=x' not in report and '&lt;img src=x' in report
    assert 'Prompt and response text omitted' in report
