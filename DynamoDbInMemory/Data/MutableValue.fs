namespace DynamoDbInMemory.Data

open System
open System.Threading
open System.Threading.Tasks
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Data.Monads.Operators

type IMutableLock<'a> =
    inherit IDisposable

    abstract member value: 'a with get, set

type private DisposableMutableLock<'a when 'a :> IDisposable> =
    { mutable value: 'a }

    interface IDisposable with
        member this.Dispose() =
            this.value.Dispose()

    interface IMutableLock<'a> with
        member this.value
            with get () = this.value
            and set value = this.value <- value

type private MutableLock<'a> =
    { mutable value: 'a }

    interface IMutableLock<'a> with
        member this.value
            with get () = this.value
            and set value = this.value <- value

    interface IDisposable with member _.Dispose() = ()

/// <summary>
/// Like a state monad, but locks and stores its state internally
/// </summary>
type MutableValue<'a> =
    private
    | Mv of IMutableLock<'a>

    interface IDisposable with
        member this.Dispose() =
            match this with
            | Mv x -> (x :> IDisposable).Dispose()

module MutableValue =
    let create x =
        { value = x; }
        :> IMutableLock<_>
        |> Mv

    let createDisposable x =
        ({ value = x; }: DisposableMutableLock<_>)
        :> IMutableLock<_>
        |> Mv

    let tryMutate (timeoutMs: int) f = function
        | Mv v ->
            if Monitor.TryEnter(v, timeoutMs) then
                try
                    let struct (x, newV) = f v.value
                    v.value <- newV
                    ValueSome x
                finally
                    Monitor.Exit v
            else ValueNone

    let mutate f =
        let ms = DynamoDbInMemory.Settings.MutableValueLockWait.TotalMilliseconds |> int
        tryMutate ms f
        >> ValueOption.defaultWith (fun _ ->
            let settings = nameof DynamoDbInMemory.Settings
            let setting = nameof DynamoDbInMemory.Settings.MutableValueLockWait
            serverError $"Unable to enter lock after {ms}ms. You can increase this timeout by modifying {settings}.{setting}")

    let rec private retryMutate' startTime i onLockFailed f x =
        match tryMutate 50 f x with
        | ValueSome x -> ValueTask<'a>(result = x)
        | ValueNone ->
            onLockFailed i (DateTimeOffset.UtcNow - startTime)
            |%|> (fun _ -> x)
            |%>>= (retryMutate' startTime (i + 1) onLockFailed f)

    /// <summary>
    /// Try a mutation. It a lock cannot be achieved after 50ms, call the onLockFailed
    /// Then try again
    /// The onLockFailed method should defer execution to another thread. It should
    /// also throw an exception if a lock cannot be achieved after too many iterations 
    /// </summary>
    let retryMutate onLockFailed = retryMutate' DateTimeOffset.UtcNow 0 onLockFailed

    let get = function | Mv v -> v.value
