[<RequireQualifiedAccess>]
module TestDynamo.Model.ClientError

open System
open System.Reflection
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.GenericMapper.Expressions
open TestDynamo.GenericMapper.Utils

let private amazonDynamoDBException =
    AppDomain.CurrentDomain.GetAssemblies()
    |> Seq.collect (fun x ->
            try
                x.GetTypes()
            with
            | :? ReflectionTypeLoadException as e -> e.Types |> Array.filter ((<>)null))
    |> Seq.filter (_.FullName >> (=)"Amazon.DynamoDBv2.AmazonDynamoDBException")
    |> Collection.tryHead
    |> Maybe.expectSomeErr "Could not find type Amazon.DynamoDBv2.AmazonDynamoDBException%s" ""

let private exn1 =
    let ctr = amazonDynamoDBException.GetConstructor([|typeof<string>|])
    
    Expr.lambda1 typeof<string> (Seq.singleton >> Expr.newObj ctr >> Expr.convert typeof<Exception>)
    |> Expr.compile
    |> Converters.fromFunc
    :?> string -> Exception
    
let private exn2 =
    let ctr = amazonDynamoDBException.GetConstructor([|typeof<string>; typeof<Exception>|])
    
    let inputT = typeof<struct (string * Exception)>
    let struct (msg, exn) = Tuple.values typeof<struct (string * Exception)>
    
    Expr.lambda1 inputT ((fun x -> [Expr.prop msg.Name x; Expr.prop exn.Name x]) >> Expr.newObj ctr >> Expr.convert typeof<Exception>)
    |> Expr.compile
    |> Converters.fromFunc
    :?> struct (string * Exception) -> Exception

let clientErrFlag' = "TestDynamo:IsClientError"
let unit' = box ()
let testDynamoException' msg inner data =
    let exn =
        match inner with
        | ValueNone -> exn1 msg
        | ValueSome e -> exn2 struct (msg, e)
        
    exn.Data.Add(clientErrFlag', unit')
        
    data
    ?|> Seq.fold (fun (s: System.Collections.IDictionary) struct (k, v) ->
        s.Add(k, v)
        s) exn.Data
    |> ignoreTyped<System.Collections.IDictionary voption>
    
    exn
        
let inline clientError msg = testDynamoException' msg ValueNone ValueNone |> raise

let inline isClientError (e: exn) = e.Data <> null && e.Data.Contains clientErrFlag'

let inline clientErrorWithInnerException msg inner = testDynamoException' msg (ValueSome inner) ValueNone |> raise
let inline clientErrorWithData data msg = testDynamoException' msg ValueNone (ValueSome data) |> raise

let inline expectSomeClientErr msg errState =
    ValueOption.defaultWith (fun _ -> sprintf msg errState |> clientError)