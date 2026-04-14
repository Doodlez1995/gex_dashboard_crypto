param(
    [int]$CollectorInterval = 600,
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8050,
    [switch]$SkipInstall,
    [switch]$OpenBrowser,
    [switch]$SkipTelegramBot
)

$argsList = @(
    "launcher.py",
    "--collector-interval", "$CollectorInterval",
    "--host", "$BindHost",
    "--port", "$Port"
)

if ($SkipInstall) {
    $argsList += "--skip-install"
}

if ($OpenBrowser) {
    $argsList += "--open-browser"
}

if ($SkipTelegramBot) {
    $argsList += "--skip-telegram-bot"
}

python @argsList
