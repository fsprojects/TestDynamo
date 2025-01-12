
if ($PSVersionTable.PSVersion.Major -lt 7 -or $PSVersionTable.PSVersion.Minor -lt 4) {
    # $ErrorActionPreference, $PSNativeCommandUseErrorActionPreference feature
    Write-Error "This script includes features which are only supported in powershell >= 7.4"
    exit 1
}

$PSNativeCommandUseErrorActionPreference = $true
$ErrorActionPreference = "Stop"


# back 1 directory from the current file
$rootDir = ($MyInvocation.MyCommand.Path | Split-Path | Split-Path)
$testResults = "$rootDir\tests\TestDynamo.Tests\TestResults"

# Run this if you don't have tooling installed
# dotnet tool install -g dotnet-reportgenerator-globaltool

Remove-Item -Recurse -Force "$testResults"
dotnet test "$rootDir\tests\TestDynamo.Tests" --collect:"XPlat Code Coverage"

ls $testResults | 
	ForEach-Object { ls $_  | 
		ForEach-Object { 
			reportgenerator `
				-reports:"$_" `
				-targetdir:"coveragereport" `
				-reporttypes:Html; .\coveragereport\index.htm

		} }