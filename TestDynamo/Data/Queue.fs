namespace TestDynamo.Data

open TestDynamo.Utils
open System.Runtime.CompilerServices

[<Struct; IsReadOnly>]
type Queue<'a> =
    private
    | Q of struct ('a list * 'a list)

module Queue =

    let private prepare = function
        | Q (_::_, _) & q
        | Q ([], []) & q -> q
        | Q ([], back) -> Q struct (List.rev back, [])

    let isEmpty = function
        | Q ([], []) -> true
        | _ -> false

    let create items = struct (items, []) |> Q
    
    type private Cache<'a>() =
        static member value: Queue<'a> = Q struct ([], [])

    let empty<'a> = Cache<'a>.value

    let private dequeue' = function
        | Q ([], _) & q -> struct (ValueNone, q)
        | Q (head::tail, back) -> struct (ValueSome head, Q struct (tail, back))

    let dequeue x = prepare x |> dequeue'

    let enqueue item = function
        | Q (front, back) -> Q struct (front, item::back)