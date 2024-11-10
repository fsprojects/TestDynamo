namespace TestDynamo.Tests

open System.Reflection
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Client
open TestDynamo.Utils
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Client.ItemMapper
open Tests.Loggers

[<DynamoDBTable("SomeKindOfOb")>]
type SomeKindOfOb() =
    
    let mutable pk: string = null
    let mutable sk: string = null
    
    [<DynamoDBHashKey>]
    member _.Pk
        with get () = pk
        and set value = pk <- value
        
    [<DynamoDBRangeKey>]
    member _.Sk
        with get () = sk
        and set value = sk <- value

type DynamoDBContextSmokeTests(output: ITestOutputHelper) =

    [<Fact(Skip = "TODO. Also, test query")>]
    let ``Query on table, with project count, returns no records`` () =
        
        task {
            // arrange
            use logger = new TestLogger(output, level = LogLevel.Trace)
            use client = TestDynamoClientBuilder.Create(logger = logger)
            // use client = buildClientWithlogLevel LogLevel.Trace (ValueSome output)
            // use client = buildClientWithlogLevel LogLevel.Trace (ValueSome output)
            let _ =
                { createStream = false
                  tableConfig =
                      { name = "SomeKindOfOb"
                        primaryIndex = struct ("Pk", ValueSome "Sk") 
                        indexes = Map.empty 
                        attributes = [struct ("Pk", AttributeType.String); struct ("Sk", AttributeType.String)]
                        addDeletionProtection = false }}
                |> (TestDynamoClient.getDatabase client).AddTable ValueNone
            
            use context = new DynamoDBContext(client)
            let cfg = DocumentModel.TableConfig("SomeKindOfOb")
            
            let yyype =
                typeof<Amazon.DynamoDBv2.DocumentModel.Table>.GetConstructors(BindingFlags.Instance ||| BindingFlags.NonPublic)
                |> Array.filter (fun x -> x.GetParameters().Length = 2 && (x.GetParameters()[0]).ParameterType = typeof<IAmazonDynamoDB> && (x.GetParameters()[1]).ParameterType = typeof<DocumentModel.TableConfig>)
                |> Array.head
                |> fun c -> c.Invoke([|box client; cfg|]) 
            
            //let uuu = new Table(ddbClient, config);
            let t = Amazon.DynamoDBv2.DocumentModel.Table.LoadTable(client, cfg)
            context.RegisterTableDefinition(t)
            
            let obj = SomeKindOfOb()
            obj.Pk <- "ThePk"
            obj.Sk <- "TheSk"
            
            // act
            do! context.SaveAsync obj
            let! obj2 = context.LoadAsync<SomeKindOfOb>("ThePk", "TheSk")

            // assert
            Assert.Equal(obj.Pk, obj2.Pk)
            Assert.Equal(obj.Sk, obj2.Sk)
        }