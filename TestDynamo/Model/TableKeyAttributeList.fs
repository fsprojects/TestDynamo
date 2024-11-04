namespace TestDynamo.Model

open System.Runtime.CompilerServices
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

/// <summary>
/// A list of key attribute names which implements equality
/// </summary>
[<Struct; IsReadOnly>]
type TableKeyAttributeList =
    private
    | Tk of struct (string * AttributeType) list

module TableKeyAttributeList =

    let empty = Tk []


    let getType x (Tk xs) = Seq.filter (fstT >> (=) x) xs |> Seq.map sndT |> Collection.tryHead

    let contains = getType >>> ValueOption.isSome

    let attributes (Tk xs) = xs

    type ExpectedActual = (struct (AttributeType * AttributeType))

    [<Struct; IsReadOnly>]
    type ValidationError =
        | MissingAttribute
        | InvalidType of ExpectedActual

    let private attributes' = attributes
    let validate attributes =
        attributes'
        >> Seq.map (fun struct (name, expectedT) ->
            MapUtils.tryFind name attributes
            ?|> (fun attr ->
                match AttributeValue.getType attr with
                | actualT when actualT = expectedT -> ValueNone
                | actualT -> InvalidType struct (expectedT, actualT) |> ValueSome)
            |> ValueOption.defaultValue (ValueSome MissingAttribute)
            ?|> (tpl name))
        >> Maybe.traverse

    let create: struct (string * AttributeType) seq -> TableKeyAttributeList =
        let throwDuplicates = function
            | [] -> ()
            | xs ->
                xs
                |> Seq.map (sprintf " * %A")
                |> Str.join "\n"
                |> sprintf "Invalid table column configuration. Found multiple definitions for cols\n%s"
                |> clientError

        Seq.distinct
        >> Collection.groupBy fstT
        >> Collection.mapSnd (List.ofSeq)
        >> Collection.partition (sndT >> List.length >> ((=)1))
        >> mapSnd throwDuplicates
        >> fstT
        >> List.collect sndT
        >> List.sort
        >> Tk