set -e

dotnet tool install -g dotnet-reportgenerator-globaltool

dotnet restore
dotnet build --no-restore

cd ./tests/TestDynamo.Tests.CSharp
dotnet test --no-build --verbosity normal

cd ../TestDynamo.Tests
echo "Running tests with coverage..."

# Print to stdout and to variable
# https://stackoverflow.com/questions/12451278/capture-stdout-to-a-variable-but-still-display-it-in-the-console
exec 5>&1

test_results=$(dotnet test --no-build --verbosity normal --collect:"XPlat Code Coverage" | tee /dev/fd/5)
cd ../..

test_results_file=$(echo "$test_results" | grep -oP "([^\s]).+/coverage\.cobertura\.xml(?=\s|$)" | tail -1)

# "grep line-rate" is a bit of a hack. The code coverage just happens 
# to be the first instance of line-rate="xxx" in the file
# Also, not rounding results, flooring because bash cannot round
code_coverage=$(cat "$test_results_file" | grep -oP "(?<=line-rate=\"0\.)\d{1,2}" | head -1)
echo "Code coverage: $code_coverage%"
echo "{\"projects\":{\"TestDynamo\":{\"coverage\":\"$code_coverage%\"}}}" > "./automatedBuildResults.json"

