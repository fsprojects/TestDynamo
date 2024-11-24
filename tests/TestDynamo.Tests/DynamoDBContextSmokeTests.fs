namespace TestDynamo.Tests

open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Utils
open Tests.ClientLoggerContainer
open Xunit
open Xunit.Abstractions
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Client.ItemMapper
open Tests.Loggers

// warning FS3511: This state machine is not statically compilable.
// A constrained generic construct occured in the resumable code specification. An alternative dyna
// mic implementation will be used, which may be slower. Consider adjusting your code to ensure this state machine is stat
// ically compilable, or else suppress this warning
#nowarn "3511"

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

    [<DynamoDBGlobalSecondaryIndexHashKey("AnIndex")>]
    member _.SomethingElse
        with get () = somethingElse
        and set value = somethingElse <- value

    [<DynamoDBGlobalSecondaryIndexRangeKey("AnIndex")>]
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

            let idx =
                { data =
                    { keys = struct ("SomethingElse", ValueSome "SomethingElseElse")
                      projectionsAreKeys = false
                      projectionCols = ValueNone }
                  isLocal = false }
            let _ =
                { createStream = false
                  tableConfig =
                      { name = "SomeKindOfOb"
                        primaryIndex = struct ("Pk", ValueSome "Sk") 
                        indexes = Map.add "AnIndex" idx Map.empty 
                        attributes =
                            [ struct ("Pk", AttributeType.String)
                              struct ("Sk", AttributeType.String)
                              struct ("SomethingElse", AttributeType.Number)
                              struct ("SomethingElseElse", AttributeType.Number)]
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

            let! loading = load() |%|> Seq.map single

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

    [<Theory>]
    [<InlineData(true)>]
    [<InlineData(false)>]
    let ``Scan and query on index`` ``is scan`` =

        task {
            // arrange
            use logger = new TestLogger(output, level = LogLevel.Trace)
            use client = TestDynamoClientBuilder.Create(logger = logger)

            let idx =
                { data =
                    { keys = struct ("SomethingElse", ValueSome "SomethingElseElse")
                      projectionsAreKeys = false
                      projectionCols = ValueNone }
                  isLocal = false }
            let _ =
                { createStream = false
                  tableConfig =
                      { name = "SomeKindOfOb"
                        primaryIndex = struct ("Pk", ValueSome "Sk") 
                        indexes = Map.add "AnIndex" idx Map.empty 
                        attributes =
                            [ struct ("Pk", AttributeType.String)
                              struct ("Sk", AttributeType.String)
                              struct ("SomethingElse", AttributeType.Number)
                              struct ("SomethingElseElse", AttributeType.Number)]
                        addDeletionProtection = false }}
                |> (TestDynamoClient.getDatabase client).AddTable ValueNone

            use context = new DynamoDBContext(client)

            let obj_on_index = SomeKindOfOb()
            obj_on_index.Pk <- "ThePk"
            obj_on_index.Sk <- "TheSk1"
            obj_on_index.SomethingElse <- System.Nullable<_>(1)
            obj_on_index.SomethingElseElse <- System.Nullable<_>(2)
            do! context.SaveAsync obj_on_index
            
            let obj_not_on_index = SomeKindOfOb()
            obj_not_on_index.Pk <- "ThePk"
            obj_not_on_index.Sk <- "TheSk2"
            do! context.SaveAsync obj_not_on_index

            // act
            let! search =
                if ``is scan``
                then
                    let req = DynamoDBOperationConfig()
                    req.IndexName <- "AnIndex"
                    context.ScanAsync<SomeKindOfOb>(Seq.empty, req)
                else
                    let req = DynamoDBOperationConfig()
                    req.IndexName <- "AnIndex"
                    context.QueryAsync(System.Nullable<_>(1), req)
                |> _.GetRemainingAsync()
            
            // assert
            let single = Assert.Single search
            Assert.Equal("TheSk1", single.Sk)
        }