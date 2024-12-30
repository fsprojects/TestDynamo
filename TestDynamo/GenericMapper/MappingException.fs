namespace TestDynamo.GenericMapper

open System
open TestDynamo.Utils

type MappingException private(path: string list, rootFrom: Type, rootTo: Type, baseFrom: Type, baseTo: Type, inner: exn) =
    inherit Exception(MappingException.msg path rootFrom rootTo baseFrom baseTo, inner |> MappingException.innerExn)

    member _.BaseFrom = baseFrom
    member _.BaseTo = baseTo
    member _.Path = path

    new(fromT: Type, toT: Type, inner: Exception) =
        MappingException ([], fromT, toT, fromT, toT, inner)

    new(property: string, fromT: Type, toT: Type, inner: MappingException) =
        MappingException (property::inner.Path, fromT, toT, inner.BaseFrom, inner.BaseTo, inner.InnerException)

    static member private innerExn: exn -> exn = function
        | null -> null
        | :? MappingException as e -> MappingException.innerExn e.InnerException 
        | e -> e

    static member private msg path rootFrom rootTo errFrom errTo =
        let prop = path |> Str.join "."
        [
            $"An error occurred when building mapper for {rootFrom.Name} => {rootTo.Name}."
            if prop = "" then "" else $"The error occured when mapping property {prop} of types {errFrom.Name} => {errTo.Name}"
            ""
            "Full types:"
            $" * From      ({rootFrom.Name}) {rootFrom.FullName}"
            $" * To        ({rootTo.Name}) {rootTo.FullName}"
            if prop = "" then "" else $" * Prop from ({errFrom.Name}) {errFrom.FullName})"
            if prop = "" then "" else $" * Prop from ({errTo.Name}) {errTo.FullName})"
        ] |> Seq.filter ((<>)"=") |> Str.join "\n"