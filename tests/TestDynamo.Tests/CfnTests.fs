namespace TestDynamo.Tests

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Serialization.CloudFormation
open TestDynamo.Utils
open Microsoft.Extensions.Logging
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Api.FSharp
open Tests.Loggers
open Tests.Requests.Queries
open TestDynamo.Serialization

#nowarn "0025"

type CfnTests(output: ITestOutputHelper) =

    let mapTask f x =
        task {
            let! x' = x
            return (f x')
        }

    let bindTask f x =
        task {
            let! x' = x
            return! (f x')
        }

    let random = randomBuilder output

    let clonedHost writer =
        task {
            // make sure that tables are populated before getting common host
            let! _ = sharedTestData ValueNone
            let hostData = commonHost.BuildCloneData()

            let globalData =
                  { databases = [{ hostData with databaseId = { regionId = "eu-west-1"}}]
                    replicationKeys = [] }

            return new GlobalDatabase(globalData, logger = writer)
        }

    static let replicate (client: AmazonDynamoDBClient) regionName tableName = function
        | ChangeType.Update ->
            let req = UpdateTableRequest()
            req.TableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicationGroupUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicationGroupMemberAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            client.UpdateTableAsync(req) |> Io.ignoreTask
        | ChangeType.CreateGlobal ->
            let req = CreateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicationGroup <- System.Collections.Generic.List()
            req.ReplicationGroup.Add(Replica())
            req.ReplicationGroup[0].RegionName <- regionName

            client.CreateGlobalTableAsync(req) |> Io.ignoreTask
        | ChangeType.UpdateGlobal ->
            let req = UpdateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicaUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicaAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            client.UpdateGlobalTableAsync(req) |> Io.ignoreTask
        | x -> invalidOp $"{x}"

    let setUp2Regions logger doReplication =
        task {
            let! host = clonedHost logger
            let client1 = TestDynamoClientBuilder.Create(host, { regionId = "eu-west-1"})
            let client2 = TestDynamoClientBuilder.Create(host, { regionId = "eu-north-1"})
            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            do!
                if doReplication
                then replicate client1 "eu-north-1" table.name ChangeType.Update
                else Task.CompletedTask

            let disposer =
                { new IDisposable with
                     member _.Dispose() =
                         host.Dispose()
                         client1.Dispose()
                         client2.Dispose() }

            return struct (table, host, client1, client2, disposer)
        }
        
    static let cfnTable' attributesOnly name =
        let opn = if attributesOnly then "" else "{"
        let close = if attributesOnly then "" else "}"
        sprintf """%s"AttributeDefinitions":[{"AttributeName":"IndexPk","AttributeType":{"Value":"N"}},{"AttributeName":"IndexSk","AttributeType":{"Value":"N"}},{"AttributeName":"TablePk","AttributeType":{"Value":"N"}},{"AttributeName":"TableSk","AttributeType":{"Value":"N"}}],"BillingMode":null,"DeletionProtectionEnabled":false,"GlobalSecondaryIndexes":[{"IndexName":"IndexName","KeySchema":[{"AttributeName":"IndexPk","KeyType":{"Value":"HASH"}},{"AttributeName":"IndexSk","KeyType":{"Value":"RANGE"}}],"OnDemandThroughput":null,"Projection":{"NonKeyAttributes":[],"ProjectionType":{"Value":"ALL"}},"ProvisionedThroughput":null}],"KeySchema":[{"AttributeName":"TablePk","KeyType":{"Value":"HASH"}},{"AttributeName":"TableSk","KeyType":{"Value":"RANGE"}}],"LocalSecondaryIndexes":[],"OnDemandThroughput":null,"ProvisionedThroughput":{"ReadCapacityUnits":0,"WriteCapacityUnits":0},"ResourcePolicy":null,"SSESpecification":null,"StreamSpecification":null,"TableClass":null,"TableName":"%s","Tags":[]%s""" opn name close
    
    static let cfnTable = cfnTable' false
    
    static let cfnGlobalTable name regions =
        let table = cfnTable' true name
        let replicas =
            regions
            |> Seq.map (fun struct (name, indexes, deletionProtection) ->
                let gsi = if indexes then """{"IndexName":"IndexName"}""" else ""
                let del = if deletionProtection then "true" else "false"
                sprintf """{"GlobalSecondaryIndexes":[%s],"DeletionProtectionEnabled":%s,"KMSMasterKeyId":null,"OnDemandThroughputOverride":null,"ProvisionedThroughputOverride":null,"Region":"%s","TableClassOverride":null}""" gsi del name)
            |> Str.join ","
            
        sprintf """{%s,"Replicas":[%s]}""" table replicas

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Cfn smoke tests`` ``force global`` =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = ``force global``
                  settings =
                      { ignoreUnsupportedResources = false } }
            
            let file =
                { region = "the-region"
                  fileJson = $"""{{"Resources":{{"TheTable":{{"Type":"AWS::DynamoDB::Table","Properties":{cfnTable "Tab1"}}}}}}}""" }
            
            // act
            let! result = CloudFormationParser.buildDatabase settings (ValueSome logger) [file]
            
            // assert
            let table =
                match result with
                | Either1 _ when ``force global`` ->
                    Assert.Fail("Expect local")
                    Unchecked.defaultof<_>
                | Either2 _ when not ``force global`` ->
                    Assert.Fail("Expect global")
                    Unchecked.defaultof<_>
                | Either1 db ->
                    Assert.Equal(db.Id.regionId, "the-region")
                    db.GetTable (ValueSome logger) "Tab1"
                | Either2 db -> (db.GetDatabase (ValueSome logger) {regionId = "the-region" }).GetTable (ValueSome logger) "Tab1"
            Assert.NotNull table
            
            let idx = table.GetIndex("IndexName")
            Assert.NotNull idx
        }

    [<Fact>]
    let ``Cfn global table test`` () =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Trace)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }
            
            let table = cfnGlobalTable "Tab11" [struct ("region-2", true, true);struct ("region-3", false, false)]
            let file =
                { region = "root-region"
                  fileJson = $"""{{"Resources":{{"TheTable":{{"Type":"AWS::DynamoDB::GlobalTable","Properties":{table}}}}}}}""" }
            
            // act
            output.WriteLine file.fileJson
            let! result = CloudFormationParser.buildDatabase settings (ValueSome logger) [file]
            
            // assert
            let struct (table1, table2, table3) =
                match result with
                | Either1 _  ->
                    Assert.Fail("Expect local")
                    Unchecked.defaultof<_>
                | Either2 db -> struct (
                    (db.GetDatabase (ValueSome logger) {regionId = "root-region" }).GetTable (ValueSome logger) "Tab11",
                    (db.GetDatabase (ValueSome logger) {regionId = "region-2" }).GetTable (ValueSome logger) "Tab11",
                    (db.GetDatabase (ValueSome logger) {regionId = "region-3" }).GetTable (ValueSome logger) "Tab11")
            
            // root table gets deletion protection by default
            Assert.True(table1.HasDeletionProtection)
            table1.GetIndexes()
            |> Seq.filter (fun x -> x.Name = "IndexName")
            |> Collection.tryHead
            |> ValueOption.isSome
            |> Assert.True
            
            Assert.True(table2.HasDeletionProtection)
            table2.GetIndexes()
            |> Seq.filter (fun x -> x.Name = "IndexName")
            |> Collection.tryHead
            |> ValueOption.isSome
            |> Assert.True
            
            Assert.False(table3.HasDeletionProtection)
            table3.GetIndexes()
            |> Seq.filter (fun x -> x.Name = "IndexName")
            |> Collection.tryHead
            |> ValueOption.isSome
            |> Assert.False
        }
        
    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Cfn multi file tests`` ``multiple regions`` =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }
            
            let file1 =
                { region = "the-region"
                  fileJson = $"""{{"Resources":{{"TheTable":{{"Type":"AWS::DynamoDB::Table","Properties":{cfnTable "Tab1"}}}}}}}""" }
            let file2 =
                { region = if ``multiple regions`` then "another-region" else "the-region"
                  fileJson = $"""{{"Resources":{{"TheTable":{{"Type":"AWS::DynamoDB::Table","Properties":{cfnTable "Tab2"}}}}}}}""" }
            
            // act
            let! result = CloudFormationParser.buildDatabase settings (ValueSome logger) [file1; file2]
            
            // assert
            let struct (table1, table2) =
                match result with
                | Either1 _ when ``multiple regions`` ->
                    Assert.Fail("Expect local")
                    Unchecked.defaultof<_>
                | Either2 _ when not ``multiple regions`` ->
                    Assert.Fail("Expect global")
                    Unchecked.defaultof<_>
                | Either1 db ->
                    Assert.Equal(db.Id.regionId, "the-region")
                    struct (db.GetTable (ValueSome logger) "Tab1", db.GetTable (ValueSome logger) "Tab2")
                | Either2 db ->
                    struct (
                        (db.GetDatabase (ValueSome logger) {regionId = "the-region" }).GetTable (ValueSome logger) "Tab1",
                        (db.GetDatabase (ValueSome logger) {regionId = "another-region" }).GetTable (ValueSome logger) "Tab2")
            
            Assert.NotNull table1
            Assert.NotNull table2
            
            let idx = table1.GetIndex("IndexName")
            Assert.NotNull idx
            let idx = table2.GetIndex("IndexName")
            Assert.NotNull idx
        }
        
    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Cfn invalid resource tests`` ``allow invalid`` =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = ``allow invalid`` } }
            
            let file =
                { region = "the-region"
                  fileJson = """{"Resources":{"TheTable":{"Type":"AWS::DynamoDB::TableXX","Properties":{}}}}""" }
                
            let execute _ = CloudFormationParser.buildDatabase settings (ValueSome logger) [file]
                
            // act
            if ``allow invalid``
            then
                let! result = execute ()
                match result with
                | Either1 _ -> ()
                | Either2 _  -> Assert.Fail("Expect local")
            else
                let! e = Assert.ThrowsAnyAsync(fun _ -> (execute ()).AsTask())
                assertError output "Unsupported construct AWS::DynamoDB::TableXX" e
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Cfn, with valid dependson, creates successfully`` flip =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }
                
            let jsonParts =
                [
                    $"""
                    "TheTable1":{{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable2",
                        "Properties":{cfnTable "Tab1"}
                    }}"""
                    $"""
                    "TheTable2":{{
                        "Type":"AWS::DynamoDB::Table",
                        "Properties":{cfnTable "Tab2"}
                    }}"""
                ]
                |> if flip then List.rev else id
                |> Str.join "," 
            
            let file =
                { region = "the-region"
                  fileJson = $"""{{
                  "Resources":{{
                    {jsonParts}
                  }}
                }}""" }
            
            // act
            let! result = CloudFormationParser.buildDatabase settings (ValueSome logger) [file]
            
            // assert
            let assertTable name =
                let table =
                    match result with
                    | Either2 _  ->
                        Assert.Fail("Expect global")
                        Unchecked.defaultof<_>
                    | Either1 db -> db.GetTable (ValueSome logger) name
                Assert.NotNull table
                
                let idx = table.GetIndex("IndexName")
                Assert.NotNull idx
                
            assertTable "Tab1"
            assertTable "Tab2"
        }

    [<Fact>]
    let ``Cfn, with three way dependency, works correctly`` () =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }
                
            let tables =
                [
                    $""""TheTable1":{{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable2",
                        "Properties":{cfnTable "Tab1"}
                    }}""";
                    $""""TheTable2":{{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable3",
                        "Properties":{cfnTable "Tab2"}
                    }}""";
                    $""""TheTable3":{{
                        "Type":"AWS::DynamoDB::Table",
                        "Properties":{cfnTable "Tab3"}
                    }}"""
                ]
                |> randomSort random
                |> Str.join ","

            let file =
                { region = "the-region"
                  fileJson = $"""{{
                  "Resources":{{{tables}}}
                  }}""" }
                
            output.WriteLine(file.fileJson)
            
            // act
            let! result = CloudFormationParser.buildDatabase settings (ValueSome logger) [file]
            
            // assert
            let assertTable name =
                let table =
                    match result with
                    | Either2 _  ->
                        Assert.Fail("Expect global")
                        Unchecked.defaultof<_>
                    | Either1 db -> db.GetTable (ValueSome logger) name
                Assert.NotNull table
                
                let idx = table.GetIndex("IndexName")
                Assert.NotNull idx
                
            assertTable "Tab1"
            assertTable "Tab2"
            assertTable "Tab3"
        }

    [<Fact>]
    let ``Cfn, with dependson self, throws`` () =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }

            let file =
                { region = "the-region"
                  fileJson = $"""{{
                  "Resources":{{
                    "TheTable1":{{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable1",
                        "Properties":{{}}
                    }}
                  }}
                }}""" }
            
            // act
            let! e = Assert.ThrowsAnyAsync(fun _ -> (CloudFormationParser.buildDatabase settings (ValueSome logger) [file]).AsTask())
            
            // assert
            assertError output "Circular reference or missing dependency" e
        }

    [<Fact>]
    let ``Cfn, with dependson missing, throws`` () =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }

            let file =
                { region = "the-region"
                  fileJson = $"""{{
                  "Resources":{{
                    "TheTable1":{{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "missing",
                        "Properties":{{}}
                    }}
                  }}
                }}""" }
            
            // act
            let! e = Assert.ThrowsAnyAsync(fun _ -> (CloudFormationParser.buildDatabase settings (ValueSome logger) [file]).AsTask())
            
            // assert
            assertError output "Circular reference or missing dependency" e
        }

    [<Fact>]
    let ``Cfn, with circular ref, throws`` () =

        task {
            // arrange
            use logger = new TestLogger(output)
            let settings =
                { alwaysCreateGlobal = false
                  settings =
                      { ignoreUnsupportedResources = false } }
                
            let tables =
                [
                    """"TheTable1":{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable2",
                        "Properties":{}
                    }""";
                    """"TheTable2":{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable3",
                        "Properties":{}
                    }""";
                    """"TheTable3":{
                        "Type":"AWS::DynamoDB::Table",
                        "DependsOn": "TheTable1",
                        "Properties":{}
                    }"""
                ]
                |> randomSort random
                |> Str.join ","

            let file =
                { region = "the-region"
                  fileJson = $"""{{
                  "Resources":{{{tables}}}
                  }}""" }
                
            output.WriteLine(file.fileJson)
            
            // act
            let! e = Assert.ThrowsAnyAsync(fun _ -> (CloudFormationParser.buildDatabase settings (ValueSome logger) [file]).AsTask())
            
            // assert
            assertError output "Circular reference or missing dependency" e
        }