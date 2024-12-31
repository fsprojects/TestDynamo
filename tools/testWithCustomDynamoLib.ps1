
# run this from the repo root directory
# AdditionalConstants is configured to add extra DefineConstants to the build (i.e. more #compiler directives)`
param(
    [Parameter(Mandatory=$true)][string]$libVersion,
    $cleanUpChanges = $true) 


$diff = (git diff)
if ($diff) {
    Write-Error "Repository must not have any changes before this test"
    exit 1
}

# back 2 directories from the current file
$rootDir = ($MyInvocation.MyCommand.Path | Split-Path | Split-Path)

$testProjects = @(
    "$rootDir\tests\TestDynamo.Tests\TestDynamo.Tests.fsproj",
    "$rootDir\tests\TestDynamo.Tests.CSharp\TestDynamo.Tests.CSharp.csproj")

function quit() {
    $testProjects |
        ForEach-Object -Process {
        
        if ($cleanUpChanges) {
            Write-Host "Reverting $_"
            git checkout "$_"
        }
    }
}

$currentVersion = "$(Get-Content "$rootDir\tests\testVersion.txt")"
if (!$? || -not $currentVersion) {
    Write-Error "Expected version file at $rootDir\tests\testVersion.txt"
    exit 1
}

$testProjects |
    ForEach-Object -Process {

        $content = "$(Get-Content $_)"

        if (!$content.Contains($currentVersion)) {
            Write-Error "Version $currentVersion does not exist in project $_"
            quit
            exit 1
        }

        $content2 = $content.Replace($currentVersion, $libVersion)
        Set-Content -Path $_ -Value $content2
    }

$additional = "NOTHING"
if ($libVersion.StartsWith("3.")) {
    $additional = "DYNAMODB_3"
}

dotnet test "-p:AdditionalConstants=$additional"
quit
if ($?) {
    Write-Host "Successfully tested with version $libVersion"
    exit 0
} else {
    exit 1
}