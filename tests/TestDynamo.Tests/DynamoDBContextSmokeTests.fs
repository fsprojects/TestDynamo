namespace TestDynamo.Tests

open System.Reflection
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
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

[<AllowNullLiteral>]
[<DynamoDBTable("SomeKindOfOb")>]
type SomeKindOfOb() =
    
    let mutable pk: string = null
    let mutable sk: string = null
    let mutable skCopy: string = null
    let mutable somethingElse: System.Nullable<int> = System.Nullable<int>()
    let mutable somethingElseElse: System.Nullable<int> = System.Nullable<int>()
    
    [<DynamoDBHashKey>]
    member _.Pk
        with get () = pk
        and set value = pk <- value
        
    [<DynamoDBRangeKey>]
    member _.Sk
        with get () = sk
        and set value = sk <- value
        
    member _.SkCopy
        with get () = skCopy
        and set value = skCopy <- value
        
    member _.SomethingElse
        with get () = somethingElse
        and set value = somethingElse <- value
        
    member _.SomethingElseElse
        with get () = somethingElseElse
        and set value = somethingElseElse <- value

type DynamoDBContextSmokeTests(output: ITestOutputHelper) =

    [<Fact>]
    let ``SmokeTest`` () =
        
        task {
            let singleton x =
                task {
                    match! x with
                    | null -> return Seq.empty
                    | x -> return Seq.singleton x
                }
                
            let seq x =
                task {
                    let! x' = x
                    return Collection.ofSeq x'
                }
                
            let single xs =
                match List.ofSeq xs with
                | [x] -> x
                | xs -> invalidOp $"Expected 1, got {xs.Length}"
                
            let scans =
                let notNotSk = ScanCondition("SkCopy", ScanOperator.NotEqual, "NotTheSk")
                [
                    [ScanCondition("SkCopy", ScanOperator.Between, "IsTheS", "IsTheSk1")]
                    [ScanCondition("SkCopy", ScanOperator.Contains, "IsTheSk")]
                    [ScanCondition("SkCopy", ScanOperator.Equal, "IsTheSk")]
                    [ScanCondition("SkCopy", ScanOperator.In, "IsTheSk", "IsTheSk1")]
                    [ScanCondition("SkCopy", ScanOperator.BeginsWith, "IsThe")]
                    [ScanCondition("SkCopy", ScanOperator.GreaterThan, "IsTheS"); notNotSk]
                    [ScanCondition("SomethingElse", ScanOperator.IsNull); notNotSk]
                    [ScanCondition("SkCopy", ScanOperator.LessThan, "IsTheSkx"); notNotSk]
                    [ScanCondition("SkCopy", ScanOperator.NotContains, "NotTheSk")]
                    [ScanCondition("SkCopy", ScanOperator.NotEqual, "NotTheSk")]
                    [ScanCondition("SkCopy", ScanOperator.IsNotNull); notNotSk]
                    [ScanCondition("SkCopy", ScanOperator.GreaterThanOrEqual, "IsTheSk"); notNotSk]
                    [ScanCondition("SkCopy", ScanOperator.LessThanOrEqual, "IsTheSk"); notNotSk]
                ]: ScanCondition list list
                
            let scansAsOperationConfig =
                scans
                |> List.map (fun ss ->
                    let sc = DynamoDBOperationConfig()
                    sc.QueryFilter <- MList<_>(ss)
                    sc)
            
            // arrange
            use logger = new TestLogger(output, level = LogLevel.Trace)
            use client = TestDynamoClientBuilder.Create(logger = logger)
            
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
            
            let obj_diversion = SomeKindOfOb()
            obj_diversion.Pk <- "ThePk"
            obj_diversion.Sk <- "NotTheSk"
            obj_diversion.SkCopy <- "NotTheSk"
            do! context.SaveAsync obj_diversion
            
            let obj = SomeKindOfOb()
            obj.Pk <- "ThePk"
            obj.Sk <- "IsTheSk"
            obj.SkCopy <- "IsTheSk"
            
            // act
            do! context.SaveAsync obj
            
            // assert
            let load _ =
                [
                    context.LoadAsync<SomeKindOfOb>("ThePk", "IsTheSk") |> singleton
                    context.QueryAsync<SomeKindOfOb>("ThePk", QueryOperator.Equal, ["IsTheSk"]).GetRemainingAsync() |> seq
                ]
                @ (scans |> List.map (fun s -> context.ScanAsync<SomeKindOfOb>(s).GetRemainingAsync() |> seq))
                @ (scansAsOperationConfig |> List.map (fun s -> context.QueryAsync<SomeKindOfOb>("ThePk", s).GetRemainingAsync() |> seq))
                |> Seq.map Io.fromTask
                |> Io.traverse
                
            let! loading = load()
            let loading = loading |> Seq.map single
                
            for obj1 in loading do
                Assert.Equal(obj.Pk, obj1.Pk)
                Assert.Equal(obj.Sk, obj1.Sk)
                
            // act
            for i, x in Seq.mapi tpl scansAsOperationConfig do
                obj.SomethingElseElse <- i
                do! context.SaveAsync(obj, x)
                let! obj2 = context.LoadAsync<SomeKindOfOb>("ThePk", "IsTheSk")
                
                Assert.Equal(obj.Pk, obj2.Pk)
                Assert.Equal(obj.Sk, obj2.Sk)
                Assert.Equal(obj.SomethingElseElse, obj2.SomethingElseElse)
                
            
            // assert
            do! context.DeleteAsync<SomeKindOfOb>("ThePk", "IsTheSk")
            let! loading = load()
                
            for obj1 in loading do
                Assert.Equal(0, Seq.length obj1)
        }