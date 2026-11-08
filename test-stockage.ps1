$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

docker compose up -d --build --wait
if ($LASTEXITCODE) { throw 'Docker Compose ne peut pas démarrer les services.' }

$broker = 'brokers=redpanda:9092'
$topics = docker compose exec -T redpanda rpk topic list -X $broker
if ($LASTEXITCODE) { throw 'Impossible de lire la liste des topics.' }
foreach ($topic in 'quotes.raw', 'quotes.minute') {
    if (-not ($topics -match "^$([regex]::Escape($topic))\s")) {
        docker compose exec -T redpanda rpk topic create $topic -p 1 -X $broker
        if ($LASTEXITCODE) { throw "Impossible de créer $topic." }
    }
}

$published = python .\simulateur.py --count 3 --symbol AAPL | ForEach-Object { ConvertFrom-Json $_ }
if ($LASTEXITCODE -or @($published).Count -ne 3) {
    throw 'Le simulateur devait publier trois cotations.'
}

$firstTimestamp = ([datetimeoffset]$published[0].event_time).UtcDateTime.ToString('o')
$sql = [uri]::EscapeDataString(
    "SELECT symbol, price, event_time FROM quotes WHERE symbol = 'AAPL' AND event_time >= '$firstTimestamp'"
)
$deadline = (Get-Date).AddSeconds(60)
do {
    try {
        $result = Invoke-RestMethod "http://localhost:9100/exec?query=$sql" -TimeoutSec 5
        $missing = @($published | Where-Object {
            $quote = $_
            -not ($result.dataset | Where-Object {
                $_[0] -eq $quote.symbol -and $_[1] -eq $quote.price -and
                [datetimeoffset]$_[2] -eq [datetimeoffset]$quote.event_time
            })
        })
        if ($missing.Count -eq 0) {
            Write-Host 'OK : les trois cotations publiées sont présentes dans QuestDB.'
            Write-Host 'Tableau Grafana : http://localhost:3100/d/cotations'
            return
        }
    } catch {
        $lastQueryError = $_
    }
    Start-Sleep -Seconds 2
} while ((Get-Date) -lt $deadline)

docker compose logs --tail 30 bridge
throw "Les trois cotations ne sont pas arrivées dans QuestDB. $lastQueryError"
