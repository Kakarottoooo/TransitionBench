$base = 'http://127.0.0.1:8765'
$body = Join-Path $PSScriptRoot 'simulation.json'
curl.exe --fail-with-body --silent --show-error "$base/api/v1/runs" -H 'Content-Type: application/json' -H 'X-TransitionBench: 1' -H "Idempotency-Key: curl-$([guid]::NewGuid())" --data-binary "@$body"
