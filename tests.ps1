param(
    $buildProjectOnly = $false,
    $buildProject = $true,
    $buildTests = $true,
    $filter = "")

$PSNativeCommandUseErrorActionPreference = $true
$ErrorActionPreference = "Stop"

if (($buildTests && $buildProject) || $buildProjectOnly) {
    dotnet build ./TestDynamo/TestDynamo.fsproj --no-dependencies --no-restore
}

if ($buildProjectOnly) {
    exit 0
}

if ($buildTests) {
    dotnet build ./tests/TestDynamo.Tests/TestDynamo.Tests.fsproj --no-dependencies --no-restore
}

if ($filter) {
    dotnet test ./tests/TestDynamo.Tests/TestDynamo.Tests.fsproj --filter $filter --no-build
} else {
    dotnet test ./tests/TestDynamo.Tests/TestDynamo.Tests.fsproj --no-build
}
