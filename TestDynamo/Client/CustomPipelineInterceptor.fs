namespace TestDynamo.Client

open System.Threading
open System.Threading.Tasks
open Amazon.Runtime
open TestDynamo
open TestDynamo.Client.MultiClientOperations
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

/// <summary>
/// A request interceptor which can be used to override or polyfill client functionality
/// </summary>
[<AllowNullLiteral>]
type IRequestInterceptor =
    /// <summary>
    /// Intercept a request. If this method returns a non-null object, the object will
    /// be used as the response of the request. If null is returned, then the request will continue
    /// as normal
    /// </summary>
    abstract member InterceptRequest: database: ApiDb -> request: obj -> CancellationToken -> ValueTask<obj>

    /// <summary>
    /// Intercept a response. If this method returns a non-null object, the object will
    /// be used as the response of the request. If null is returned, then the response will continue as normal.
    ///
    /// Responses are most likely mutable AmazonWebServiceResponses, and you can modify them as required
    /// </summary>
    abstract member InterceptResponse: database: ApiDb -> request: obj -> response: obj -> CancellationToken -> ValueTask<obj>

type CustomPipelineInterceptor(
    db: ApiDb,
    interceptor: IRequestInterceptor) =

    let mutable awsLogger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
    let mutable innerHandler = Unchecked.defaultof<IPipelineHandler>
    let mutable outerHandler = Unchecked.defaultof<IPipelineHandler>
    static let noInterceptorResult = ValueTask<obj>(result = null).Preserve()

    static let vtIsDefault vt =
        if vt = Unchecked.defaultof<ValueTask<_>>
        then Io.retn ValueNone
        else vt |%|> CSharp.toOption

    static let cast x: AmazonWebServiceResponse = x

    member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
        let c = executionContext.RequestContext.CancellationToken
        interceptor.InterceptRequest db executionContext.RequestContext.OriginalRequest c
        |> vtIsDefault
        |%>>= function
            | ValueNone -> innerHandler.InvokeAsync<'a> executionContext |> Io.fromTask
            | ValueSome (:? 'a as x) -> x |> Io.retn
            | ValueSome x -> invalidOp $"InterceptRequest is expected to return a {typeof<'a>}"
        |%>>= fun response ->
            interceptor.InterceptResponse db executionContext.RequestContext.OriginalRequest response c
            |> vtIsDefault
            |%|> function
                | ValueSome (:? 'a as x) -> x
                | ValueSome x -> invalidOp $"InterceptResponse is expected to return a {typeof<'a>}"
                | ValueNone -> response

    interface IPipelineHandler with

        member _.Logger
            with get () = awsLogger
            and set value = awsLogger <- value
        member _.InnerHandler
            with get () = innerHandler
            and set value = innerHandler <- value
        member _.OuterHandler
            with get () = outerHandler
            and set value = outerHandler <- value

        member this.InvokeSync(executionContext: IExecutionContext) =
            this.InvokeAsync<AmazonWebServiceResponse> executionContext
            |> Io.execute
            |> ignoreTyped<AmazonWebServiceResponse>

        member this.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
            (this.InvokeAsync<'a> executionContext).AsTask()
