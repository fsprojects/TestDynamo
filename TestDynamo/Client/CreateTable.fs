module TestDynamo.Client.CreateTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Client
open TestDynamo.Client.DescribeTable.Local
open TestDynamo.Model

type MList<'a> = System.Collections.Generic.List<'a>

let private buildGsiSchema' (x: GlobalSecondaryIndex) = buildGsiSchema x.KeySchema x.Projection

let buildLsiSchema tablePk keySchema =
    match fromKeySchema (keySchema |> List.ofSeq) with
    | _, ValueNone -> clientError "Sort key is mandatory for local secondary index"
    | indexPk, ValueSome _ when indexPk <> tablePk -> clientError "Partition key must be the same as table partition key for local secondary index"
    | _, ValueSome _ -> buildGsiSchema keySchema

let private buildLsiSchema' tablePk (x: LocalSecondaryIndex) = buildLsiSchema tablePk x.KeySchema x.Projection

let fromAttributeDefinitions attr =
    Seq.map (fun (x: AttributeDefinition) ->
        match x.AttributeType with
        | ``type`` when ``type``.Value = ScalarAttributeType.S.Value -> AttributeType.String
        | ``type`` when ``type``.Value = ScalarAttributeType.N.Value -> AttributeType.Number
        | ``type`` when ``type``.Value = ScalarAttributeType.B.Value -> AttributeType.Binary
        | ``type`` -> clientError $"Invalid attribute type {``type``} for key schema element {x.AttributeName}"
        |> tpl x.AttributeName) attr

let buildStreamConfig (streamSpecification: StreamSpecification) =
    match streamSpecification with
    | null -> ValueNone
    | x when not x.StreamEnabled -> DeleteStream |> ValueSome
    | x when x.StreamViewType = null -> CreateStream |> ValueSome
    | x ->
        match StreamDataType.tryParse x.StreamViewType.Value with
        | ValueNone -> clientError $"Invalid {nameof x.StreamViewType} \"{x.StreamViewType.Value}\""
        | ValueSome _ & x -> CreateStream |> ValueSome

let addLsi key oldV newV =
    match oldV with
    | ValueNone -> newV
    | ValueSome _ -> clientError $"Duplicate key definition {key}"

let inputs (req: CreateTableRequest) =

    let indexes =
        Seq.map (fun x -> { isLocal = false; data = buildGsiSchema' x } |> tpl x.IndexName) req.GlobalSecondaryIndexes
        |> MapUtils.fromTuple

    let (struct (pk, _) & primaryIndex) = fromKeySchema (req.KeySchema |> List.ofSeq)
    let indexes =
        Seq.fold (fun s x -> { isLocal = true; data = buildLsiSchema' pk x } |> flip (MapUtils.change addLsi x.IndexName) s) indexes req.LocalSecondaryIndexes

    let tableConfig =
        { name = req.TableName |> CSharp.mandatory "TableName is mandatory"
          primaryIndex = primaryIndex
          addDeletionProtection = req.DeletionProtectionEnabled 
          attributes = fromAttributeDefinitions req.AttributeDefinitions |> List.ofSeq
          indexes = indexes }

    { tableConfig = tableConfig
      createStream = buildStreamConfig req.StreamSpecification |> ValueOption.isSome }

let output awsAccountId databaseId (table: TableDetails) =

    let output = Shared.amazonWebServiceResponse<CreateTableResponse>()
    output.TableDescription <- tableDescription awsAccountId databaseId ValueNone table TableStatus.ACTIVE
    output