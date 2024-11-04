namespace DynamoDbInMemory.Data.BasicStructures

open System.Runtime.CompilerServices

[<IsReadOnly; Struct>]
type CreateOrDelete<'a> =
    | Create of c: 'a
    | Delete of 'a

[<IsReadOnly; Struct>]
type NonEmptyList<'a> =
    private | L of 'a list

module NonEmptyList =
    let ofHeadTail head tail = head::tail |> L
    let prepend head (L tail) = head::tail |> L
    let singleton x = [x] |> L
    let ofList x =
        match x with
        | [] -> invalidOp "List cannot be empty"
        | xs -> L xs

    let unwrap (L xs) = xs

    let head (L xs) = List.head xs

    let pop = function
        | L (head::(_::_ & tail)) -> struct (head, ofList tail |> ValueSome)
        | xs -> struct (head xs, ValueNone)

    let concat (L xs) (L ys) = xs @ ys |> L