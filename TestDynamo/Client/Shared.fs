module TestDynamo.Client.Shared

open System.Net
open TestDynamo
open TestDynamo.Model
open TestDynamo.Utils

let (!!<) = ValueSome
let (<!!>) x propName = ClientError.expectSomeClientErr "%s property is mandatory" propName x

let noneifyStrings = function
    | ValueSome null
    | ValueSome ""
    | ValueNone -> ValueNone
    | x -> x

module ResponseHeaders =
    let contentLength = TestDynamo.Settings.AmazonWebServiceResponse.ResponseContentLength
    let httpStatusCode = HttpStatusCode.OK
    let responseMetadata =
        { ChecksumAlgorithm = !!<Settings.AmazonWebServiceResponse.ChecksumAlgorithm
          ChecksumValidationStatus = !!<Settings.AmazonWebServiceResponse.ChecksumValidationStatus
          RequestId = !!<TestDynamo.Settings.AmazonWebServiceResponse.RequestId.Invoke()
          Metadata = !!<Map.empty }: TestDynamo.GeneratedCode.Dtos.ResponseMetadata

/// <summary>
/// Tools to generate a list of names like: (#t1, :t1), (#t2, :t2), ...
/// The value of this component is in cached strings
/// </summary>
module NameValueEnumerator =

    let private next' struct (prefix, i) =
        let t = i + 1
        struct (struct ($"#{prefix}{t}", $":{prefix}{t}"), t)

    let private cacheMax = 2000
    let private next =
        let k (x: struct (string * int)) = x
        memoize (ValueSome (cacheMax / 2, cacheMax + 1)) k next' >> sndT

    let infiniteNames prefix =
        seq {
            // allow for about 10 prefixes to be cached @ 200 items each
            let mutable i = cacheMax / 10
            let mutable tkn = next struct (prefix, 0)
            while true do
                yield fstT tkn
                tkn <- (if i > 0 then next else next') (struct (prefix, sndT tkn))
                i <- i - 1
        }

module GetUtils =

    let buildProjection projectionExpression (attributesToGet: string array voption) =

        match struct (projectionExpression, attributesToGet) with
        | ValueSome _, ValueSome arr when arr.Length > 0 ->
            ClientError.clientError $"Cannot use ProjectionExpression and AttributesToGet in the same request"
        | ValueNone, ValueNone
        | ValueNone, ValueSome [||] -> ValueNone
        | ValueSome prj, ValueNone
        | ValueSome prj, ValueSome _ -> struct (prj, id) |> ValueSome
        | ValueNone, ValueSome xs ->
            xs
            |> Collection.zip (NameValueEnumerator.infiniteNames "p")
            |> Seq.map (fun struct (struct (id, _), attr) -> struct (id, struct (id, attr)))
            |> Collection.unzip
            |> mapFst (Str.join ",")
            |> mapSnd (flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)))
            |> ValueSome