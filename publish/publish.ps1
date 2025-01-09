param(
    [Parameter(Mandatory=$true)][string]$version,
    $testOldVersions = $true)

# back 2 directories from the current file
$rootDir = ($MyInvocation.MyCommand.Path | Split-Path | Split-Path)

$versionFile = "$rootDir\v$version.txt"
$versionTag = "v$version"
$versionBranch = "working-v$version"

$testDynamoDll = "$rootDir\TestDynamo\TestDynamo.fsproj"

$publishApps = @(
    $testDynamoDll,
    "$rootDir\TestDynamo.Lambda\TestDynamo.Lambda.fsproj",
    "$rootDir\TestDynamo.Serialization\TestDynamo.Serialization.fsproj")

$testApps = @(
    "$rootDir\tests\TestDynamo.Tests\TestDynamo.Tests.fsproj",
    "$rootDir\tests\TestDynamo.Tests.CSharp\TestDynamo.Tests.CSharp.csproj")

function Mid-Execution {
    Test-Path $versionFile
}

Write-Host ""
Write-Host "### Step 1: test project with older versions of dynamo db" -ForegroundColor Green
$mx = Mid-Execution
if (!$mx -and $testOldVersions) {
    $dynamoVersions = @(
        @("DYNAMO_V379", "3.7.404.11"),   # 2024-12-26
        @("DYNAMO_V370", "3.7.0")         # 2021-03-06
        # TODO
        # @("DYNAMO_V359", "3.5.4.38"),     # 2021-03-25
        # @("DYNAMO_V350", "3.5.0"),        # 2020-08-20
        # @("DYNAMO_V339", "3.3.5")         # 2020-08-19
        )

    $dynamoVersions |
        ForEach-Object -Process {
            Write-Host "Testing with dynamodb version $($_[0])/$($_[1])"
            Invoke-Expression "$rootDir\tools\testWithCustomDynamoLib.ps1 -libVersionId `"$($_[0])`" -libVersion `"$($_[1])`""
            if (!$?) {
                exit 1
            }
        }
} elseif ($testOldVersions) {
    Write-Host "Skipping old version test. Mid execution"
}

Write-Host ""
Write-Host "### Step 2: make sure repo is clean. This process will create tags and branches and commit files" -ForegroundColor Green
$diff = (git diff)
if ($diff) {
    Write-Error "Repository must not have any changes before pack"
    exit 1
}

if (!$mx) {

    Write-Host ""
    Write-Host "### Step 3: merge GeneratedCode and TestDynamo projects" -ForegroundColor Green
    node "$rootDir\publish\project-parser\addGeneratedDtosIntoTestDynamo.js" `
        --testDynamo "$testDynamoDll" `
        --generatedCode "$rootDir\TestDynamo.GeneratedCode\TestDynamo.GeneratedCode.fsproj"
    if (!$?) { exit 1 }

    Write-Host ""
    Write-Host "### Step 4: final test, with merged code, in release mode" -ForegroundColor Green
    dotnet test "$rootDir" -c Release
    if (!$?) { exit 1 }
    
    Write-Host ""
    Write-Host "### Step 5: create release state (tag, branch and version file)" -ForegroundColor Green
    git tag "$versionTag"
    git checkout -b "$versionBranch"
    New-Item -path "$rootDir" -name "v$version.txt" -type "file" -value ""
    git add $versionFile
    git commit -m "Added version file"
} elseif ((git branch --show-current) -ne $versionBranch) {
    Write-Error "Incorrect branch"
    exit 1
} else {
    Write-Host ""
    Write-Host "### Skipping steps 3 - 5" -ForegroundColor Green
}

Write-Host ""
Write-Host "### Step 6: change dependant projects to reference the new release version of their dependencies" -ForegroundColor Green
Write-Host "Pre processing project files"
node "$rootDir\publish\project-parser\reformatFsProjForRelease.js" @($testApps + $publishApps) --version "$version"
if (-not $?) { exit 1 }

git add @($testApps + $publishApps)
git commit -m "fsproj changes"

function handle-pack-errors ($i, $err) {
    Write-Error "ERROR $err"
    if ($i -gt 30) {
        Write-Error "Too many failures. Quitting"
        exit 1
    }
    
    # NU1603 means that a required nuget version is not present yet.
    # This will happen after the core project is deployed and it is still indexing
    if (!($err -match "NU1603" || $err -match "NU1102")) {
        exit 1
    }

    Write-Host "Waiting for 1min before next attempt"
    Start-Sleep -Seconds 60
}

$nugetKey = ""
$publishApps |
    ForEach-Object -Process {

        $i = 0
        while ($true) {
            Write-Host "Packing $_, attempt $i"
            $project = $_

            if (Select-String -Path $versionFile -Pattern $project -SimpleMatch) {
                Write-Host "Project $_ already done"
                return
            }

            Write-Host ""
            Write-Host "### Step 7: pack and release" -ForegroundColor Green
            Write-Host "### This tep might take a while, as dependant projects will be bloced while nuget indexes their dependencies"
            $packResult = (dotnet restore $_ --no-cache)
            if (-not $?) {
                handle-pack-errors $i $packResult
            } else {
                $packResult = (dotnet pack `
                    "$project" `
                    --configuration Release `
                    -p:PackageVersion="$version")
                    
                if (-not $?) {
                    handle-pack-errors $i $packResult
                } else {
                    break
                }
            }

            $i = $i + 1
        }

        $match = select-string "Successfully created package '([\w-\.\\/:]+).nupkg'" -inputobject $packResult
        if ($match) {
            $nugetPackage = "$($match.Matches.Groups[1].Captures[0].Value).nupkg"
        } else {
            Write-Error "Could not find nupkg file name"
            exit 1
        }

        $match = select-string "Successfully created package '([^']+).snupkg'" -inputobject $packResult
        if ($match) {
            $symbolsPackage = "$($match.Matches.Groups[1].Captures[0].Value).snupkg"
        } else {
            Write-Error "Could not find snupkg file name"
            exit 1
        }

        if (!$nugetKey) { $nugetKey = Read-Host "Enter a nuget key" -MaskInput }
        @($nugetPackage, $symbolsPackage) | ForEach-Object -Process {
            dotnet nuget push `
                -s https://api.nuget.org/v3/index.json `
                "$_" `
                -k "$nugetKey" `
                --skip-duplicate
                
            if (-not $?) {
                Write-Error "ERROR"
                exit 1
            }
        }

        add-content -Path "$versionFile" -Value "$project"
        git add $versionFile
        git commit -m "Deployed $project"
    }

Write-Host ""
Write-Host "### Step 8: run tests a final time, with reference to new deployed versions" -ForegroundColor Green
$i = 0
while ($True) {
    Write-Host "Infinite test loop. Will keep going until success"
    $success = $true
    $testApps |
        ForEach-Object -Process {
            Write-Host "Testing $_"
            dotnet restore "$_" --no-cache
            dotnet test "$_"

            if (-not $?) { $success = $false}
        }

    if ($success) { break; }
    
    Write-Host "Sleep 60"
    Start-Sleep -Seconds 60
}

git push origin "$versionTag"
git reset HEAD --hard
git checkout main
git branch -D $versionBranch
Write-Host "Deploy complete"
