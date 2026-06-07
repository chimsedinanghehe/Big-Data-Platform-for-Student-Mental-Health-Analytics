param(
    [string]$VmHost = "34.21.211.62",
    [string]$VmUser = "Admin",
    [string]$SshKey = "$HOME\.ssh\google_compute_engine",
    [int]$LocalPort = 9092,
    [int]$RemotePort = 9092
)

$ErrorActionPreference = "Stop"

function Test-LocalPort {
    param([int]$Port)
    return [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
}

if (Test-LocalPort $LocalPort) {
    Write-Host "Kafka tunnel is already listening on 127.0.0.1:$LocalPort"
    exit 0
}

if (-not (Test-Path $SshKey)) {
    throw "Missing SSH key: $SshKey"
}

$ssh = (Get-Command ssh.exe -ErrorAction Stop).Source
$process = Start-Process -FilePath $ssh -WindowStyle Hidden -PassThru -ArgumentList @(
    "-N",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=30",
    "-o", "ServerAliveCountMax=3",
    "-o", "ExitOnForwardFailure=yes",
    "-i", $SshKey,
    "-L", "127.0.0.1:${LocalPort}:localhost:${RemotePort}",
    "${VmUser}@${VmHost}"
)

Start-Sleep -Seconds 4
if ($process.HasExited -or -not (Test-LocalPort $LocalPort)) {
    throw "Kafka SSH tunnel failed to start."
}

Write-Host "Kafka tunnel started. PID=$($process.Id), 127.0.0.1:$LocalPort -> ${VmHost}:$RemotePort"
