namespace TestDynamo.Tests

open System
open System.IO
open System.Linq
open System.Reflection
open AutoFixture
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open FluentAssertions
open Tests.Loggers
open Tests.Utils
open Xunit
open Xunit.Abstractions
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.GenericMapper
open TestDynamo.GenericMapper.DtoMappers
open TestDynamo.Tests.RequestItemTestUtils

#nowarn "0025"

module MappingTestGenerator =

    let rec attributeValue' (random: Random) depth (fixture: Fixture): DynamoAttributeValue voption =
        if depth <= 0
        then ValueNone
        else

        let attr = DynamoAttributeValue()
        match random.Next 3 with
        | 0 ->
            attributeValue' random (depth - 1) fixture
            ?|> fun v ->
                attr.M <- Dictionary()
                attr.IsMSet <- true

                attr.M.Add(fixture.Create<string>(), v)

        | 1 ->
            attributeValue' random (depth - 1) fixture
            ?|> fun v ->
                attr.L <- MList()
                attr.IsLSet <- true

                attr.L.Add v

        | 2 ->
            match random.Next 8 with
            | 0 -> attr.S <- fixture.Create<string>()
            | 1 -> attr.N <- fixture.Create<decimal>() |> toString
            | 2 -> attr.B <- fixture.Create<MemoryStream>()
            | 3 -> attr.NULL <- true
            | 4 ->
                attr.BOOL <- fixture.Create<bool>()
                attr.IsBOOLSet <- true

            | 5 ->
                attr.SS <- fixture.Create<MList<string>>()
                attr.IsSSSet <- true

            | 6 ->
                attr.BS <- fixture.Create<MList<MemoryStream>>()
                attr.IsBSSet <- true

            | 7 ->
                attr.NS <- fixture.Create<MList<decimal>>() |> Seq.map toString |> Enumerable.ToList
                attr.IsNSSet <- true

            |> ValueSome
        | x -> invalidOp (x.ToString())
        ?|> fun _ -> attr

    let attributeValue (random: Random) (fixture: Fixture) =

        [1..10]
        |> Seq.map (fun _ -> attributeValue' random 6 fixture)
        |> Maybe.traverse
        |> Collection.tryHead
        ?|>? fun _ ->
            let attr = DynamoAttributeValue()
            attr.S <- fixture.Create<string>()
            attr

    let registerConst<'c> random (fixture: Fixture) (cs: 'c seq) =
        fixture.Register<'c>(fun _ -> cs |> randomSeqItems 1 (asLazy true) random |> Seq.head)

    module Fixture =    
        open Amazon.DynamoDBv2

        let fixture random =
            let fixture = Fixture()
            fixture.Register<MemoryStream>(fun _ -> new MemoryStream(fixture.Create<byte array>()))
            fixture.Register<Amazon.DynamoDBv2.Model.AttributeValue>(fun _ -> attributeValue random fixture)

            [S3SseAlgorithm.AES256; S3SseAlgorithm.KMS] |> registerConst random fixture
            [ScalarAttributeType.B; ScalarAttributeType.N; ScalarAttributeType.S] |> registerConst random fixture
            [ApproximateCreationDateTimePrecision.MICROSECOND; ApproximateCreationDateTimePrecision.MILLISECOND] |> registerConst random fixture
            [BackupStatus.DELETED; BackupStatus.AVAILABLE; BackupStatus.CREATING] |> registerConst random fixture
            [BatchStatementErrorCodeEnum.AccessDenied; BatchStatementErrorCodeEnum.DuplicateItem; BatchStatementErrorCodeEnum.ValidationError] |> registerConst random fixture
            [BillingMode.PROVISIONED; BillingMode.PAY_PER_REQUEST] |> registerConst random fixture
            [ContinuousBackupsStatus.ENABLED; ContinuousBackupsStatus.DISABLED] |> registerConst random fixture
            [BackupType.USER; BackupType.SYSTEM; BackupType.AWS_BACKUP] |> registerConst random fixture
            [PointInTimeRecoveryStatus.ENABLED; PointInTimeRecoveryStatus.DISABLED] |> registerConst random fixture
            [ContributorInsightsStatus.FAILED; ContributorInsightsStatus.ENABLED; ContributorInsightsStatus.DISABLING] |> registerConst random fixture
            [GlobalTableStatus.ACTIVE; GlobalTableStatus.UPDATING; GlobalTableStatus.DELETING] |> registerConst random fixture
            [IndexStatus.CREATING; IndexStatus.DELETING; IndexStatus.UPDATING] |> registerConst random fixture
            [ExportFormat.ION; ExportFormat.DYNAMODB_JSON] |> registerConst random fixture
            [KeyType.HASH; KeyType.RANGE] |> registerConst random fixture
            [ReplicaStatus.CREATING; ReplicaStatus.UPDATING; ReplicaStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS] |> registerConst random fixture
            [DestinationStatus.UPDATING; DestinationStatus.ENABLING; DestinationStatus.ENABLE_FAILED] |> registerConst random fixture
            [TimeToLiveStatus.DISABLING; TimeToLiveStatus.ENABLING; TimeToLiveStatus.DISABLED] |> registerConst random fixture
            [ExportStatus.FAILED; ExportStatus.COMPLETED; ExportStatus.IN_PROGRESS] |> registerConst random fixture
            [ProjectionType.ALL;ProjectionType.KEYS_ONLY; ProjectionType.INCLUDE] |> registerConst random fixture
            [TableClass.STANDARD; TableClass.STANDARD_INFREQUENT_ACCESS] |> registerConst random fixture
            [AttributeAction.ADD; AttributeAction.PUT; AttributeAction.DELETE] |> registerConst random fixture
            [ContributorInsightsAction.ENABLE; ContributorInsightsAction.DISABLE] |> registerConst random fixture
            [ReturnValuesOnConditionCheckFailure.NONE; ReturnValuesOnConditionCheckFailure.ALL_OLD] |> registerConst random fixture
            [ReturnConsumedCapacity.NONE; ReturnConsumedCapacity.TOTAL; ReturnConsumedCapacity.INDEXES] |> registerConst random fixture
            [StreamViewType.KEYS_ONLY; StreamViewType.NEW_IMAGE; StreamViewType.NEW_AND_OLD_IMAGES] |> registerConst random fixture
            [SSEType.AES256; SSEType.KMS] |> registerConst random fixture
            [Select.COUNT; Select.SPECIFIC_ATTRIBUTES; Select.ALL_PROJECTED_ATTRIBUTES] |> registerConst random fixture
            [ConditionalOperator.OR; ConditionalOperator.AND] |> registerConst random fixture
            [ReturnValue.NONE; ReturnValue.ALL_NEW] |> registerConst random fixture
            [ReturnItemCollectionMetrics.NONE; ReturnItemCollectionMetrics.SIZE] |> registerConst random fixture
            [ComparisonOperator.GE; ComparisonOperator.LT; ComparisonOperator.BETWEEN] |> registerConst random fixture
            [InputFormat.CSV; InputFormat.DYNAMODB_JSON; InputFormat.ION] |> registerConst random fixture
            [InputCompressionType.GZIP; InputCompressionType.NONE; InputCompressionType.ZSTD] |> registerConst random fixture
            [ExportViewType.NEW_IMAGE; ExportViewType.NEW_AND_OLD_IMAGES] |> registerConst random fixture
            [ExportType.FULL_EXPORT; ExportType.INCREMENTAL_EXPORT] |> registerConst random fixture
            [SSEStatus.ENABLED; SSEStatus.DISABLED; SSEStatus.ENABLING] |> registerConst random fixture
            [ImportStatus.FAILED; ImportStatus.COMPLETED; ImportStatus.IN_PROGRESS] |> registerConst random fixture
            [TableStatus.CREATING; TableStatus.ARCHIVING; TableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS] |> registerConst random fixture
            [BackupTypeFilter.ALL; BackupTypeFilter.USER; BackupTypeFilter.AWS_BACKUP] |> registerConst random fixture

            fixture

    let generator () =

        let allAssemblies =
            [|
                typeof<DynamodbTypeAttribute>.Assembly
                typeof<Amazon.DynamoDBv2.AmazonDynamoDBRequest>.Assembly
                typeof<Amazon.Runtime.AmazonWebServiceRequest>.Assembly
            |]

        let allTypes =
            allAssemblies
            |> Seq.collect _.GetTypes()
            |> Array.ofSeq

        let getType (t: string) =
            allAssemblies
            |> Seq.map (fun a -> a.GetType t)
            |> CSharp.sanitizeSeq
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Can't find %s" t

        let types =
            allTypes
            |> Seq.map (fun t ->
                match t.GetCustomAttribute<DynamodbTypeAttribute>() with
                | null -> ValueNone
                | x -> x.Name |> getType |> flip tpl t |> ValueSome)
            |> Maybe.traverse
            |> Seq.filter (fun struct (x, y) ->
                not x.IsInterface
                && not y.IsInterface
                && not x.IsAbstract
                && not y.IsAbstract
                && x <> typeof<ConstantClass>
                && y <> typeof<ConstantClass>
                && x <> typeof<AmazonDynamoDBRequest>
                && y <> typeof<AmazonDynamoDBRequest>
                && x <> typeof<AmazonWebServiceRequest>
                && y <> typeof<AmazonWebServiceRequest>
                && x <> typeof<Amazon.Runtime.Endpoints.Endpoint>
                && y <> typeof<Amazon.Runtime.Endpoints.Endpoint>)
            |> Array.ofSeq

        Assert.True(Array.length types > 50)
        types
        |> Collection.mapSnd (fun x ->
            match x.GetGenericArguments() with
            | [||] -> x
            | gs ->
                let gArgs =
                    gs |> Array.map (function
                        | x when x.Name = "attr" -> typeof<AttributeValue>
                        | x -> invalidOp $"Unknown generic name {x.Name}")
                x.MakeGenericType gArgs)

    let generator2() =
        [|
            struct (
                typeof<Amazon.DynamoDBv2.Model.WriteRequest>,
                typeof<TestDynamo.GeneratedCode.Dtos.WriteRequest<AttributeValue>>)
        |] |> Seq.ofArray

    type MappingTestData () =
        inherit TestData<struct (Type * Type)>(generator)

    // does nothing special, but resets recursion limit
    type DynamoAttributeValueComparer() =

        interface IEqualityComparer<DynamoAttributeValue> with
            member _.GetHashCode x = 0
            member _.Equals (x: DynamoAttributeValue, y: DynamoAttributeValue) =
                x.Should().BeEquivalentTo(y,
                    because = "",
                    config = _.Using<MemoryStream, MemoryStreamComparer>())
                |> fun _ -> true

    and MemoryStreamComparer() =
        static let cmp =  Comparison.arrayComparer<byte> 2

        interface IEqualityComparer<MemoryStream> with
            member _.GetHashCode x = cmp.GetHashCode (x.ToArray())
            member _.Equals (x: MemoryStream, y: MemoryStream) = cmp.Equals (x.ToArray(), y.ToArray())

    type TestExecutor() =

        static member MapToAndFrom<'from, 't> random =

            let start = 
                let start = (Fixture.fixture random).Create<'from>()

                // set some awkward props
                match box start with
                | :? Amazon.DynamoDBv2.Model.ScanRequest as sc ->
                    sc.IsLimitSet <- true
                    sc.IsSegmentSet <- true
                    sc.IsTotalSegmentsSet <- true
                | :? Amazon.DynamoDBv2.Model.GetItemResponse as ir ->
                    ir.IsItemSet <- true
                | :? Amazon.DynamoDBv2.Model.QueryRequest as qr ->
                    qr.IsLimitSet <- true
                | _ -> ()

                start

            let mapped = DtoMappers.mapDto<'from, 't> start
            let ``end`` = DtoMappers.mapDto<'t, 'from> mapped

            match box start with
            // nothing to compare
            | :? Amazon.DynamoDBv2.Model.DescribeEndpointsRequest -> ()
            | :? Amazon.DynamoDBv2.Model.DescribeLimitsRequest -> ()
            | _ ->
                ``end``
                    .Should()
                    .BeEquivalentTo(
                        start,
                        because = "",
                        config = (fun x ->
                            x
                               .Using<MemoryStream, MemoryStreamComparer>()
                               .Using<DynamoAttributeValue, DynamoAttributeValueComparer>()))
                |> ignoreTyped<AndConstraint<Primitives.ObjectAssertions>>

type MappingTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    [<Theory>]
    [<ClassData(typeof<MappingTestGenerator.MappingTestData>)>]
    let ``Test all mappable classes`` struct (tFrom: Type, tTo: Type) =

        let m = typeof<MappingTestGenerator.TestExecutor>.GetMethod(
            nameof MappingTestGenerator.TestExecutor.MapToAndFrom,
            BindingFlags.Static ||| BindingFlags.Public)

        output.WriteLine($"{tFrom} {tTo}")
        m.MakeGenericMethod([|tFrom; tTo|]).Invoke(null, [|random |> box|])
        |> fun x -> x :?> unit

    [<Fact>]
    let ``Test from is set method`` () =

        // arrange
        let data = Amazon.DynamoDBv2.Model.BatchExecuteStatementRequest()

        // act
        let result = DtoMappers.mapDto<
            Amazon.DynamoDBv2.Model.BatchExecuteStatementRequest,
            TestDynamo.GeneratedCode.Dtos.BatchExecuteStatementRequest<AttributeValue>> data

        // assert
        Assert.NotNull(data.Statements)
        Assert.Equal(ValueNone, result.Statements)

    [<Fact>]
    let ``Test from is set prop`` () =

        // arrange
        let data = Amazon.DynamoDBv2.Model.GetItemResponse()

        // act
        let result1 = DtoMappers.mapDto<
            Amazon.DynamoDBv2.Model.GetItemResponse,
            TestDynamo.GeneratedCode.Dtos.GetItemResponse<AttributeValue>> data

        data.IsItemSet <- true
        let result2 = DtoMappers.mapDto<
            Amazon.DynamoDBv2.Model.GetItemResponse,
            TestDynamo.GeneratedCode.Dtos.GetItemResponse<AttributeValue>> data

        // assert
        Assert.True(result1.Item.IsNone)
        Assert.True(result2.Item.IsSome)

    // particular use case with bug
    [<Theory>]
    //[<InlineData(false)>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Test ScanRequest, from AWS, is limit set`` ``is set`` =

        // arrange
        let data = Amazon.DynamoDBv2.Model.ScanRequest()
        if ``is set`` then data.Limit <- 10

        // act
        let result1 = DtoMappers.mapDto<
            Amazon.DynamoDBv2.Model.ScanRequest,
            TestDynamo.GeneratedCode.Dtos.ScanRequest<AttributeValue>> data

        // assert
        Assert.Equal(``is set``, data.GetType().GetProperty("IsLimitSet").GetValue(data) :?> bool)
        Assert.Equal(``is set``, data.GetType().GetMethod("IsSetLimit", BindingFlags.Instance ||| BindingFlags.NonPublic).Invoke(data, [||]) :?> bool)
        Assert.Equal(``is set``, result1.Limit.IsSome)

    // particular use case with bug
    [<Theory>]
    //[<InlineData(false)>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Test ScanRequest, to AWS, is limit set`` ``is set`` =

        // arrange
        let data =
            // TODO: can these Nones be mapped to empty lists automatically for certain props?
            { AttributesToGet = ValueNone
              ConditionalOperator = ValueNone
              ConsistentRead = ValueNone
              ExclusiveStartKey = ValueNone
              ExpressionAttributeNames = ValueNone
              ExpressionAttributeValues = ValueNone
              FilterExpression = ValueNone
              IndexName = ValueNone
              Limit = if ``is set`` then ValueSome 1 else ValueNone
              ProjectionExpression = ValueNone
              ReturnConsumedCapacity = ValueNone
              ScanFilter = ValueNone
              Segment = ValueNone
              Select = ValueNone
              TableName = ValueNone
              TotalSegments = ValueNone }: ScanRequest<AttributeValue>

        // act
        let result1 = DtoMappers.mapDto<
            TestDynamo.GeneratedCode.Dtos.ScanRequest<AttributeValue>,
            Amazon.DynamoDBv2.Model.ScanRequest> data

        // assert
        Assert.Equal(``is set``, result1.GetType().GetProperty("IsLimitSet").GetValue(result1) :?> bool)
        Assert.Equal(``is set``, result1.GetType().GetMethod("IsSetLimit", BindingFlags.Instance ||| BindingFlags.NonPublic).Invoke(result1, [||]) :?> bool)
        Assert.Equal((if ``is set`` then 1 else 0), result1.Limit)
