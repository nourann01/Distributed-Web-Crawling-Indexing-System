# AWS Configuration - Replace with your instance IDs
$masterInstanceId = "i-047033d21ff38ca28"
$crawlerInstanceId = "i-0cb93926ed4cac173"
$indexerInstanceId = "i-09fcaafe723338557"
$crawler1InstanceId = "i-0c282c1f789d10ac4"
$crawler2InstanceId = "i-0f7ad34b7fe33aea2"
$crawler3InstanceId = "i-0ed8b6b9168cfe25e"

# Key paths
$masterKeyPath = "C:\Users\Mohamed\Downloads\master-key.pem"
$crawlerKeyPath = "C:\Users\Mohamed\Downloads\crawler-key1.pem"
$indexerKeyPath = "C:\Users\Mohamed\Downloads\indexer-node.pem"
$crawler1KeyPath = "C:\Users\Mohamed\Downloads\crawler-key1.pem"
$crawler2KeyPath = "C:\Users\Mohamed\Downloads\crawler-key1.pem"
$crawler3KeyPath = "C:\Users\Mohamed\Downloads\crawler-key1.pem"

function Show-Banner {
    Write-Host "`n`n===============================================" -ForegroundColor Cyan
    Write-Host "       DISTRIBUTED WEB CRAWLER SYSTEM" -ForegroundColor Yellow
    Write-Host "===============================================" -ForegroundColor Cyan
    Write-Host "A scalable multi-node crawler architecture`n" -ForegroundColor DarkCyan
}

function Get-CrawlerParameters {
    Write-Host "CRAWLER CONFIGURATION" -ForegroundColor Green
    Write-Host "---------------------" -ForegroundColor Green
    
    # Get seed URLs
    $seedURLs = $null
    while (-not $seedURLs) {
        $seedURLsInput = Read-Host -Prompt "Enter seed URLs (space-separated)"
        $seedURLs = $seedURLsInput.Trim()
        if (-not $seedURLs) {
            Write-Host "Please enter at least one URL to start crawling." -ForegroundColor Red
        }
    }
    
    # Get depth limit
    $depthLimit = 0
    while ($depthLimit -lt 1) {
        try {
            $depthLimitInput = Read-Host -Prompt "Enter maximum crawl depth per website (recommended 1-5)"
            $depthLimit = [int]$depthLimitInput
            if ($depthLimit -lt 1) {
                Write-Host "Depth must be at least 1." -ForegroundColor Red
            }
        } catch {
            Write-Host "Please enter a valid number." -ForegroundColor Red
        }
    }
    
    # Get restricted URLs
    $restrictedURLsInput = Read-Host -Prompt "Enter URLs to restrict (space-separated, leave empty to skip)"
    $restrictedURLs = $restrictedURLsInput.Trim()
    
    # Return all parameters
    return @{
        SeedURLs = $seedURLs
        DepthLimit = $depthLimit
        RestrictedURLs = $restrictedURLs
    }
}

Show-Banner

Write-Host "SYSTEM STARTUP" -ForegroundColor Magenta
Write-Host "-------------" -ForegroundColor Magenta

Write-Host "Starting EC2 instances..." -ForegroundColor Yellow
aws ec2 start-instances --instance-ids $masterInstanceId $crawlerInstanceId $indexerInstanceId | Out-Null

Write-Host "Waiting for instances to start..." -ForegroundColor Yellow
Write-Host "This may take a minute or two. Please be patient." -ForegroundColor DarkYellow
aws ec2 wait instance-running --instance-ids $masterInstanceId $crawlerInstanceId $indexerInstanceId | Out-Null

Write-Host "Getting instance IP addresses..." -ForegroundColor Yellow
$masterIP = (aws ec2 describe-instances --instance-ids $masterInstanceId --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
$crawlerIP = (aws ec2 describe-instances --instance-ids $crawlerInstanceId --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
$indexerIP = (aws ec2 describe-instances --instance-ids $indexerInstanceId --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

Write-Host "`nINSTANCE STATUS" -ForegroundColor Green
Write-Host "-------------" -ForegroundColor Green
Write-Host "Master Node:   " -NoNewline -ForegroundColor White; Write-Host "$masterIP" -ForegroundColor Cyan
Write-Host "Crawler Node:  " -NoNewline -ForegroundColor White; Write-Host "$crawlerIP" -ForegroundColor Cyan
Write-Host "Indexer Node:  " -NoNewline -ForegroundColor White; Write-Host "$indexerIP" -ForegroundColor Cyan
Write-Host "Additional Workers: Ready for auto-scaling" -ForegroundColor White
Write-Host ""

# Get the crawler parameters
$params = Get-CrawlerParameters

# Display summary of configuration
Write-Host "`nCRAWLER SUMMARY" -ForegroundColor Green
Write-Host "---------------" -ForegroundColor Green
Write-Host "Seeds:  " -NoNewline -ForegroundColor White
Write-Host "$($params.SeedURLs)" -ForegroundColor Yellow
Write-Host "Depth:  " -NoNewline -ForegroundColor White
Write-Host "$($params.DepthLimit)" -ForegroundColor Yellow

if ($params.RestrictedURLs) {
    Write-Host "Restrictions: " -NoNewline -ForegroundColor White
    Write-Host "$($params.RestrictedURLs)" -ForegroundColor Yellow
} else {
    Write-Host "Restrictions: " -NoNewline -ForegroundColor White
    Write-Host "None" -ForegroundColor Yellow
}

# Prepare command arguments
$masterArgs = "python3 master_node.py --urls $($params.SeedURLs) --depth $($params.DepthLimit)"
if ($params.RestrictedURLs) {
    $masterArgs += " --restricted $($params.RestrictedURLs)"
}

# Create the batch script
$batchContent = @"
@echo off
color 0A
cls
echo ===============================================
echo       DISTRIBUTED WEB CRAWLER SYSTEM
echo ===============================================
echo.
echo Starting crawler nodes...
echo.
start "Master Node" cmd /k "color 0B && echo MASTER NODE && echo =========== && echo. && ssh -o StrictHostKeyChecking=no -i "$masterKeyPath" ubuntu@$masterIP "$masterArgs""
timeout /t 2 >nul
start "Crawler Node" cmd /k "color 09 && echo CRAWLER NODE && echo ============ && echo. && ssh -o StrictHostKeyChecking=no -i "$crawlerKeyPath" ubuntu@$crawlerIP "python3 crawler_node.py""
timeout /t 2 >nul
start "Indexer Node" cmd /k "color 0C && echo INDEXER NODE && echo ============ && echo. && ssh -o StrictHostKeyChecking=no -i "$indexerKeyPath" ubuntu@$indexerIP "python3 indexer_node.py""
echo.
echo All nodes started in separate windows.
echo.
echo AUTO-SCALING: Additional workers will be launched automatically based on queue depth.
echo.
echo Press any key to stop all instances when you're finished...
pause
"@

$tempBatchFile = "$env:TEMP\start-crawler-temp.bat"
$batchContent | Out-File -FilePath $tempBatchFile -Encoding ascii

# ------------------------------
# Background job to monitor queue and launch crawlers 1–3
Write-Host "`nACTIVATING AUTO-SCALING SYSTEM" -ForegroundColor Magenta
Write-Host "---------------------------" -ForegroundColor Magenta
Write-Host "Auto-scaler will monitor queue depth and launch additional crawlers automatically." -ForegroundColor DarkYellow

Start-Job -ScriptBlock {
    param($crawler1InstanceId, $crawler2InstanceId, $crawler3InstanceId, $crawlerKeyPath)

    function Get-QueueDepth {
        $json = aws sqs get-queue-attributes `
            --queue-url "https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo" `
            --attribute-names ApproximateNumberOfMessages | ConvertFrom-Json
        return [int]$json.Attributes.ApproximateNumberOfMessages
    }

    function Start-CrawlerNode {
        param($instanceId, $keyPath, $nodeName)

        Write-Host "Starting $nodeName..." -ForegroundColor Green
        aws ec2 start-instances --instance-ids $instanceId | Out-Null
        aws ec2 wait instance-running --instance-ids $instanceId | Out-Null

        $maxRetries = 10
        $retry = 0
        do {
            Start-Sleep -Seconds 6
            $crawlerIP = (aws ec2 describe-instances --instance-ids $instanceId --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
            Write-Host "Waiting for public IP... Attempt $($retry + 1): $crawlerIP"
            $retry++
        } while (($crawlerIP -eq "None" -or $crawlerIP -eq $null) -and $retry -lt $maxRetries)

        if ($crawlerIP -ne "None" -and $crawlerIP -ne $null) {
            Write-Host "Started $nodeName at $crawlerIP" -ForegroundColor Green
            $terminalColor = switch ($nodeName) {
                "Crawler Node 1" { "0E" }
                "Crawler Node 2" { "0D" }
                "Crawler Node 3" { "0F" }
                default { "07" }
            }
            Start-Process "cmd.exe" -ArgumentList "/k", "color $terminalColor && echo $nodeName && echo ============ && echo. && ssh -o StrictHostKeyChecking=no -i `"$keyPath`" ubuntu@$crawlerIP `"python3 crawler_node.py`""
            return $true
        } else {
            Write-Host "Failed to get public IP for $nodeName." -ForegroundColor Red
            return $false
        }
    }

    $started1 = $false
    $started2 = $false
    $started3 = $false
    $startTime = Get-Date

    Write-Host "Auto-scaler activated and monitoring queue depth..."
    Write-Host "Queue depth thresholds: >100 = Node 1, >200 = Node 2, >300 = Node 3"

    while ($true) {
        $depth = Get-QueueDepth
        Write-Host "Current Queue Depth: $depth URLs"

        if (-not $started1 -and $depth -gt 100) {
            Write-Host "Queue depth exceeded 100 - Activating Crawler Node 1" -ForegroundColor Yellow
            $started1 = Start-CrawlerNode -instanceId $crawler1InstanceId -keyPath $crawlerKeyPath -nodeName "Crawler Node 1"
        }

        if (-not $started2 -and $depth -gt 200) {
            Write-Host "Queue depth exceeded 200 - Activating Crawler Node 2" -ForegroundColor Yellow
            $started2 = Start-CrawlerNode -instanceId $crawler2InstanceId -keyPath $crawlerKeyPath -nodeName "Crawler Node 2"
        }

        if (-not $started3 -and $depth -gt 300) {
            Write-Host "Queue depth exceeded 300 - Activating Crawler Node 3" -ForegroundColor Yellow
            $started3 = Start-CrawlerNode -instanceId $crawler3InstanceId -keyPath $crawlerKeyPath -nodeName "Crawler Node 3"
        }

        Start-Sleep -Seconds 10

        if (((Get-Date) - $startTime).TotalMinutes -gt 30) {
            Write-Host "Auto-scaling monitoring timeout reached (30 minutes)."
            break
        }
    }

    Write-Host "Queue monitoring ended."
} -ArgumentList $crawler1InstanceId, $crawler2InstanceId, $crawler3InstanceId, $crawler1KeyPath | Out-Null
# ------------------------------

# Launch batch file to start main 3 nodes
Write-Host "`nLAUNCHING CRAWLER SYSTEM" -ForegroundColor Green
Write-Host "----------------------" -ForegroundColor Green
Write-Host "Starting main crawler nodes now..." -ForegroundColor Yellow
Start-Process $tempBatchFile -Wait

# Shutdown all instances when user finishes
Write-Host "`nSHUTTING DOWN SYSTEM" -ForegroundColor Red
Write-Host "------------------" -ForegroundColor Red
Write-Host "Stopping all EC2 instances..." -ForegroundColor Yellow
aws ec2 stop-instances --instance-ids $masterInstanceId $crawlerInstanceId $indexerInstanceId $crawler1InstanceId $crawler2InstanceId $crawler3InstanceId | Out-Null

Write-Host "`nCOMPLETE" -ForegroundColor Green
Write-Host "---------" -ForegroundColor Green
Write-Host "Web crawler session completed. All instances stopped." -ForegroundColor Green
Write-Host "Thank you for using the Distributed Web Crawler System!" -ForegroundColor Cyan