namespace TestDynamo.Tests

open System

[<CustomComparison; CustomEquality>]
type DynamoDbVersion =
    | Latest
    | DYNAMO_V379
    | DYNAMO_V370
    | DYNAMO_V359
    | DYNAMO_V350
    | DYNAMO_V339
    
    static member private versionNumber = function
        | Latest      -> Int32.MaxValue
        | DYNAMO_V379 -> 50
        | DYNAMO_V370 -> 40
        | DYNAMO_V359 -> 30
        | DYNAMO_V350 -> 20
        | DYNAMO_V339 -> 10
        
    // probably not needed, but F# needs it
    override this.Equals obj =
        match obj with
        | :? DynamoDbVersion as y -> DynamoDbVersion.versionNumber this = DynamoDbVersion.versionNumber y
        | _ -> false
        
    // probably not needed, but F# needs it
    override this.GetHashCode() = DynamoDbVersion.versionNumber this
    
    interface IComparable with
        member this.CompareTo obj =
            match obj with
            | :? DynamoDbVersion as y -> DynamoDbVersion.versionNumber this - DynamoDbVersion.versionNumber y
            | _ -> invalidArg "obj" "cannot compare value of different types"

module DynamoDbVersion =
    
    let private versions: DynamoDbVersion list =
        [
#if DYNAMO_V379
            DYNAMO_V379
#endif
#if DYNAMO_V370
            DYNAMO_V370
#endif
#if DYNAMO_V359
            DYNAMO_V359
#endif
#if DYNAMO_V350
            DYNAMO_V350
#endif
#if DYNAMO_V339
            DYNAMO_V339
#endif
        ]
    
    let version: DynamoDbVersion =
        match versions with
        | [] ->
#if DYNAMODB_3
            invalidOp $"Invalid dynamodb version setter combination. DYNAMODB_3 should be automatically set if any of the other versions are set"
#else
            Latest
#endif
        | [x] ->
#if DYNAMODB_3
            x
#else
            invalidOp $"Invalid dynamodb version setter combination. DYNAMODB_3 should not be set manually"
#endif
        | xs -> invalidOp $"Found multiple dynamodb versions {xs}"
        
    let isLatestVersion = version = Latest
        
    let isVersionOrLess v =
        compare version v <= 0
        
     