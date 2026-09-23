$ErrorActionPreference = 'Stop'
$project = Split-Path -Parent $PSScriptRoot
& (Join-Path $project '.venv/Scripts/python.exe') (Join-Path $PSScriptRoot 'clean_install_check.py')
if ($LASTEXITCODE -ne 0) { throw 'Fresh package acceptance failed' }
