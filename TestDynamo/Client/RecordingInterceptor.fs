namespace TestDynamo.Client

open System
open Amazon.Runtime
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

type Recording =
    { startTime: DateTimeOffset
      endTime: DateTimeOffset
      request: AmazonWebServiceRequest
      response: Either<AmazonWebServiceResponse, exn> }
    
    with
    member this.IsSuccess = match this.response with | Either1 _ -> true | Either2 _ -> false
    member this.IsException = not this.IsSuccess
    
    member this.SuccessResponse =
        match this.response with
        | Either1 x -> x
        | Either2 _ -> invalidOp "This response was a failure. Use the IsSuccess property before getting the SuccessResponse"
    member this.Exception =
        match this.response with
        | Either1 _ -> invalidOp "This response was not a failure. Use the IsException property before getting the Exception"
        | Either2 x -> x

type RecordingInterceptor() =

    let mutable awsLogger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
    let mutable innerHandler = Unchecked.defaultof<IPipelineHandler>
    let mutable outerHandler = Unchecked.defaultof<IPipelineHandler>
    let requests = MList<Recording>()
    
    let onSuccess reqStart input output =
        requests.Add(
            { startTime = reqStart
              endTime = DateTimeOffset.UtcNow 
              request = input
              response = Either1 output })
        output
    let onFailure reqStart input e = 
        requests.Add(
            { startTime = reqStart
              endTime = DateTimeOffset.UtcNow 
              request = input
              response = Either2 e })
    
    member _.Requests = Seq.skip 0 requests
    
    member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
        
        let exe () =
            innerHandler.InvokeAsync executionContext
            |> Io.fromTask
           
        let start = DateTimeOffset.UtcNow
        onFailure start executionContext.RequestContext.OriginalRequest 
        |> Io.onError exe
        |%|> onSuccess start executionContext.RequestContext.OriginalRequest

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
            (this.InvokeAsync<'a> executionContext |%|> fun x -> x :?> 'a).AsTask()
