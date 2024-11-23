module TestDynamo.GenericMapper.DtoMappers

open System
open System.Collections.Generic
open System.Linq
open System.Reflection
open System.Text.RegularExpressions
open Microsoft.FSharp.Core
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Utils
open TestDynamo.GenericMapper.Expressions
open TestDynamo.GenericMapper.Utils
open TestDynamo.GenericMapper.Utils.Converters
open TestDynamo.GenericMapper.Utils.Reflection
open TestDynamo.GenericMapper.Utils.Reflection.Elevated

#nowarn "3390"

type IDictionary<'a, 'b> = System.Collections.Generic.IDictionary<'a, 'b>
type AttributeType = TestDynamo.Model.AttributeType
type AttributeValue = TestDynamo.Model.AttributeValue
type AttributeSet = TestDynamo.Model.AttributeSet
type AttributeListType = TestDynamo.Model.AttributeListType

let private none _ = ValueNone    
let private constTrue = Expr.constant true

type private DebugUtils() =

    // this env variable must be set before application startup
    static let traceDtoMapping =
        System.Environment.GetEnvironmentVariable "TRACE_DTO_MAPPING"
        |> CSharp.emptyStringToNull
        |> CSharp.toOption
        ?|> (fun x -> Regex.IsMatch(x, @"^\s*(0|false|no|null)\s*$") |> not)
        ?|? false

    // keep private. Does not have any traceDtoMapping protection
    static member debug'<'a> (f: 'a -> string, x: 'a) =
        Console.WriteLine($"DtoMap {f x}")
        x

    static member debug<'a, 's> (f: struct ('s * 'a) -> string) (s: 's) (x: 'a) =
#if DEBUG
        if traceDtoMapping then DebugUtils.debug'(f, struct (s, x)) |> sndT
        else x
#else
        x
#endif

    static member debugExpression<'a> (f: 'a -> string) (expr: Expr) =
#if DEBUG
        if not traceDtoMapping then expr
        elif (expr.Type <> typeof<'a>) then expr
        else

        let m =
            { isStatic = true
              t = typeof<DebugUtils>
              name = nameof DebugUtils.debug'
              args = [funcType [typeof<'a>; typeof<string>]; typeof<'a>]
              returnType = typeof<'a>
              methodGenerics = [typeof<'a>]
              classGenerics = []
              caseInsensitiveName = false } |> method

        [Expr.constant f; expr]
        |> Expr.callStatic m
#else
        expr
#endif

module IsSet =

    let possibleIfSet name =
        [
            $"{name}IsSet"
            $"IsSet{name}"
            $"Is{name}Set"
        ]

    let isIsSet (name: string) =
        name.StartsWith("isset", StringComparison.OrdinalIgnoreCase)
        || name.EndsWith("isset", StringComparison.OrdinalIgnoreCase)
        || (name.StartsWith("is", StringComparison.OrdinalIgnoreCase)
            && name.EndsWith("set", StringComparison.OrdinalIgnoreCase))

    let ifSetMember requireRead requireWrite (pFrom: PropertyInfo) =

        possibleIfSet pFrom.Name
        |> Seq.map (fun name ->
            [ pFrom.DeclaringType.GetProperty(name, BindingFlags.Instance ||| BindingFlags.Public)
              pFrom.DeclaringType.GetProperty(name, BindingFlags.Instance ||| BindingFlags.NonPublic) ]
            |> Seq.filter ((<>) null)
            |> if requireRead then Seq.filter _.CanRead else id
            |> if requireWrite then Seq.filter _.CanWrite else id
            |> Collection.tryHead
            ?|> (Either1 >> ValueSome)
            ?|>? fun _ ->
                 if requireWrite
                 then ValueNone
                 else 
                    { isStatic = false
                      t = pFrom.DeclaringType
                      name = name
                      args = []
                      returnType = typeof<bool>
                      methodGenerics = []
                      classGenerics = []
                      caseInsensitiveName = false }
                    |> tryMethod
                    ?|> Either2)
        |> Maybe.traverse
        |> Collection.tryHead

    let ifSetExpression struct (eFrom: Expr, pFrom: PropertyInfo) =
        ifSetMember true false pFrom
        ?|> (
            Either.map1Of2 (piName >> flip Expr.prop eFrom)
            >> Either.map2Of2 (flip1To3 Expr.call eFrom [])
            >> Either.reduce)

module ToAttributeValue =
    type private AttributeValueBuilder<'a>(
          S: 'a -> string voption,
          SS: 'a -> string seq voption,
          N: 'a -> decimal voption,
          NS: 'a -> decimal seq voption,
          B: 'a -> byte array voption,
          BS: 'a -> byte array seq voption,
          l: 'a -> 'a seq voption,
          M: 'a -> Map<string, 'a> voption,
          BOOL: 'a -> bool voption,
          NULL: 'a -> bool) =

        member private this.attrV = this.attributeValue

        member private this.mapMapper = fun _ -> this.attributeValue

        member this.attributeValue (x: 'a) =
            match NULL x with
            | true -> AttributeValue.Null
            | false ->
            match S x with
            | ValueSome s -> AttributeValue.String s
            | ValueNone ->
            match N x with
            | ValueSome n -> AttributeValue.Number n
            | ValueNone ->
            match B x with
            | ValueSome b -> AttributeValue.Binary b
            | ValueNone ->
            match BOOL x with
            | ValueSome b -> AttributeValue.Boolean b
            | ValueNone ->
            match SS x with
            | ValueSome ss -> Seq.map AttributeValue.String ss |> AttributeSet.create |> AttributeValue.HashSet
            | ValueNone ->
            match NS x with
            | ValueSome ns -> Seq.map AttributeValue.Number ns |> AttributeSet.create |> AttributeValue.HashSet
            | ValueNone ->
            match BS x with
            | ValueSome bs -> Seq.map AttributeValue.Binary bs |> AttributeSet.create |> AttributeValue.HashSet
            | ValueNone ->
            match M x with
            | ValueSome m -> m |> Map.map this.mapMapper |> AttributeValue.HashMap
            | ValueNone ->
            match l x with
            | ValueSome l -> l |> Seq.map this.attrV |> Array.ofSeq |> AttributeListType.CompressedList |> AttributeValue.AttributeList
            | ValueNone -> clientError "AttributeValue object has no data"

    let private isNullAccessor (t: Type) =

        Expr.lambda1 t (fun arg ->

            [ t.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
              t.GetProperties(BindingFlags.Instance ||| BindingFlags.NonPublic) ]
            |> Seq.concat
            |> Seq.filter (fun x ->
                "Null".Equals(x.Name, StringComparison.OrdinalIgnoreCase)
                || "IsNull".Equals(x.Name, StringComparison.OrdinalIgnoreCase))
            |> Seq.filter (_.PropertyType >> (=) typeof<bool>)
            |> Seq.filter _.CanRead
            |> Seq.map (_.Name >> flip Expr.prop arg)
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Expected property Null or IsNull on type %A" t)
        |> Expr.compile
        |> fromFunc

    let private propAccessor invokePropMap (mapFromProp: PropertyInfo) (expectedType: Type) =

        let optType = getValueOptionType expectedType |> Maybe.expectSomeErr "Expected type %O to be a voption" expectedType

        Expr.lambda1 mapFromProp.DeclaringType (fun mapFromParam ->
            let isSet =
                IsSet.ifSetExpression struct (mapFromParam, mapFromProp)
                ?|> fun isSet mapped -> Expr.condition isSet mapped (valueNone optType |> Expr.constantT expectedType)
                ?|? id

            Expr.prop mapFromProp.Name mapFromParam
            |> flip (invokePropMap mapFromProp.Name) expectedType
            |> isSet)
        |> Expr.compile
        |> fromFunc

    let mapper invokePropMap (eFrom: Expr) (tTo: Type): Expr voption =
        if tTo <> typeof<AttributeValue> then ValueNone
        else

        let builderT =
            typedefof<AttributeValueBuilder<_>>.MakeGenericType(eFrom.Type)

        let attributeValue =
            { isStatic = false
              t = builderT
              name = "attributeValue"
              args = [eFrom.Type]
              returnType = typeof<AttributeValue>
              classGenerics = []
              methodGenerics = []
              caseInsensitiveName = true } |> method

        [
            struct ("S", typeof<string voption>)
            struct ("SS", typeof<string seq voption>)
            struct ("N", typeof<decimal voption>)
            struct ("NS", typeof<decimal seq voption>)
            struct ("B", typeof<byte array voption>)
            struct ("BS", typeof<byte array seq voption>)
            struct ("L", typedefof<_ voption>.MakeGenericType([|typedefof<_ seq>.MakeGenericType([|eFrom.Type|])|]))
            struct ("M", typedefof<_ voption>.MakeGenericType([|typedefof<Map<_, _>>.MakeGenericType([|typeof<string>; eFrom.Type|])|]))
            struct ("BOOL", typeof<bool voption>)
        ]
        |> Seq.map (mapFst (fun name ->
            [ eFrom.Type.GetProperty(name, BindingFlags.Instance ||| BindingFlags.Public)
              eFrom.Type.GetProperty(name, BindingFlags.Instance ||| BindingFlags.NonPublic) ]
            |> Seq.filter ((<>) null)
            |> Seq.filter _.CanRead
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Expected property on type %A" struct (name, eFrom.Type)))
        |> Seq.map (uncurry (propAccessor invokePropMap))
        |> flip Seq.append [isNullAccessor eFrom.Type]
        |> createInstance builderT
        |> Expr.constant
        |> flip (Expr.call attributeValue) [|eFrom|]
        |> ValueSome

module FromAttributeValue =
    type private AttributeValueBuilder2<'a>(
          S: string -> 'a,
          SS: string seq -> 'a,
          N: decimal -> 'a,
          NS: decimal seq -> 'a,
          B: byte array -> 'a,
          BS: byte array seq -> 'a,
          l: 'a seq -> 'a,
          M: Map<string, 'a> -> 'a,
          BOOL: bool -> 'a,
          NULL: unit -> 'a
          ) =

        member private this.attrV = this.attributeValue

        member private this.mapMapper = fun _ -> this.attributeValue

        member this.attributeValue (attr: AttributeValue) =
            match attr with
            | AttributeValue.String x -> S x
            | AttributeValue.Number x -> N x
            | AttributeValue.Binary x -> B x
            | AttributeValue.HashSet x when AttributeSet.getSetType x = AttributeType.String -> SS (AttributeSet.asStringSeq x)
            | AttributeValue.HashSet x when AttributeSet.getSetType x = AttributeType.Number -> NS (AttributeSet.asNumberSeq x)
            | AttributeValue.HashSet x when AttributeSet.getSetType x = AttributeType.Binary -> BS (AttributeSet.asBinarySeq x)
            | AttributeValue.HashSet x -> invalidOp $"Invalid set type {AttributeSet.getSetType x}"
            | AttributeValue.AttributeList x -> AttributeListType.asSeq x |> Seq.map this.attrV |> l
            | AttributeValue.HashMap x -> M (x |> Map.map this.mapMapper)
            | AttributeValue.Boolean x -> BOOL x
            | AttributeValue.Null -> NULL()

    let private restructurePropTpl struct (struct (x, y), z) = struct (x, z, y)
    let private constTrue = Expr.constant true

    let propBuilder invokePropMap mapFromProp (inputType: Type) (mapToProp: PropertyInfo) =

        // need to map L and M values inside AttributeValueBuilder2 to
        // avoid infinite loops when building mappers
        let inputType =
            if mapToProp.Name = "L"
            then
                let enmType =
                    getEnumerableType mapToProp.PropertyType
                    |> Maybe.expectSomeErr "Expected type %O to be a seq" mapToProp.PropertyType
                typedefof<_ seq>.MakeGenericType([|enmType|])
            elif mapToProp.Name = "M"
            then
                let kvpType =
                    getEnumerableType mapToProp.PropertyType
                    |> Maybe.expectSomeErr "Expected type %O to be a seq" mapToProp.PropertyType
                typedefof<Map<_, _>>.MakeGenericType(kvpType.GetGenericArguments())
            else inputType

        // S: string -> 'a,
        Expr.lambda1 inputType (fun mapFromParam ->

            let convertedParam =
                if mapToProp.Name = "NULL"
                // Constant true works because IsNULLSet will turn this on or off
                then Expr.constant true
                else invokePropMap mapFromProp mapFromParam mapToProp.PropertyType

            let assignValue =
                Expr.prop mapToProp.Name
                >> flip Expr.assign convertedParam

            let assignIsSet =
                IsSet.ifSetMember false true mapToProp
                ?>>= (
                    Either.map1Of2 ValueSome
                    >> Either.map2Of2 (fun _ -> ValueNone) 
                    >> Either.reduce)
                ?|> (
                    piName
                    >> Expr.prop
                    >>> flip Expr.assign constTrue)
                ?|? id

            mapToProp.DeclaringType.GetConstructors()
            |> Seq.filter (_.GetParameters() >> _.Length >> (=)0)
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Expected type %A to have a parameterless constructor" mapToProp.DeclaringType
            |> flip Expr.newObj []
            |> Expr.mutate [ assignValue; assignIsSet ])
        |> Expr.compile
        |> fromFunc

    let mapper invokePropMap (eFrom: Expr) (tTo: Type): Expr voption =
        if eFrom.Type <> typeof<AttributeValue> then ValueNone
        else

        let builderT =
            typedefof<AttributeValueBuilder2<_>>.MakeGenericType(tTo)

        let attributeValue =
            { isStatic = false
              t = builderT
              name = "attributeValue"
              args = [typeof<AttributeValue>]
              returnType = tTo
              classGenerics = []
              methodGenerics = []
              caseInsensitiveName = true }
            |> method

        [
            struct ("S", typeof<string>)
            struct ("SS", typeof<string seq>)
            struct ("N", typeof<decimal>)
            struct ("NS", typeof<decimal seq>)
            struct ("B", typeof<byte array>)
            struct ("BS", typeof<byte array seq>)
            struct ("L", typedefof<_ seq>.MakeGenericType([|eFrom.Type|]))
            struct ("M", typedefof<Map<_, _>>.MakeGenericType([|typeof<string>; eFrom.Type|]))
            struct ("BOOL", typeof<bool>)
            struct ("NULL", typeof<unit>)
        ]
        |> Seq.map (mapFst (fun name ->
            [ tTo.GetProperty(name, BindingFlags.Instance ||| BindingFlags.Public)
              tTo.GetProperty(name, BindingFlags.Instance ||| BindingFlags.NonPublic) ]
            |> Seq.filter ((<>) null)
            |> Seq.filter _.CanRead
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Expected property on type %A" struct (name, tTo)
            |> tpl name))
        |> Seq.map (restructurePropTpl >> uncurry3 (propBuilder invokePropMap))
        |> Array.ofSeq
        |> createInstance builderT
        |> Expr.constant
        |> flip (Expr.call attributeValue) [eFrom]
        |> ValueSome

module VOption =

    type private Utils() =

        static member valueOptionOrDefault<'a> (x: 'a voption): 'a =
            match x with | ValueSome x' -> x' | ValueNone -> Unchecked.defaultof<_>

        static member toValueOptionR<'a when 'a : null> (x: 'a) =
            match x with | null -> ValueNone | x -> ValueSome x

        static member toValueOptionV<'a> (x: 'a) = ValueSome x

        static member valueOptionMap<'a, 'b>(f: 'a -> 'b, x: 'a voption) = ValueOption.map f x

    let private vOptionMap (tTo: Type) (mapper: Expr) (opt: Expr) =
        let optType = getValueOptionType opt.Type |> Maybe.expectSomeErr "Expected type %O to be a voption" opt.Type

        { isStatic = true
          t = typeof<Utils>
          name = nameof Utils.valueOptionMap
          args = [mapper.Type; opt.Type]
          returnType = typedefof<_ voption>.MakeGenericType(mapper.Type.GetGenericArguments()[1])
          methodGenerics = mapper.Type.GetGenericArguments() |> List.ofArray
          classGenerics = []
          caseInsensitiveName = false }
        |> method
        |> flip Expr.callStatic [mapper; opt]

    let private toValueOption (e: Expr) =
        match getValueOptionType e.Type with
        | ValueSome _ -> e
        | ValueNone ->
            { isStatic = true
              t = typeof<Utils>
              name =
                  if e.Type.IsClass || e.Type.IsInterface
                  then nameof Utils.toValueOptionR
                  else nameof Utils.toValueOptionV
              args = [e.Type]
              returnType = typedefof<_ voption>.MakeGenericType([|e.Type|])
              methodGenerics = [e.Type]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [e]

    let unwrapOption (option: Expr) =
        let opt = getValueOptionType option.Type |> Maybe.expectSomeErr "Expected type %O to be a voption" option.Type

        // 'a voption -> 'a
        { isStatic = true
          t = typeof<Utils>
          name = nameof Utils.valueOptionOrDefault
          args = [option.Type]
          returnType = opt
          methodGenerics = [opt]
          classGenerics = []
          caseInsensitiveName = false }
        |> method
        |> flip Expr.callStatic [option]

    let mapper buildMapperDelegate invokeMap (eFrom: Expr) (tTo: Type) =
        match struct (getValueOptionType eFrom.Type, getValueOptionType tTo) with
        | ValueNone, ValueNone -> ValueNone
        | ValueSome optFrom, ValueNone ->
            let innerMapper =
                buildMapperDelegate struct (optFrom, tTo)
                |> fromFunc
                |> Expr.constant

            vOptionMap tTo innerMapper eFrom
            |> unwrapOption
            |> ValueSome
        | ValueNone, ValueSome optTo ->
            invokeMap eFrom optTo
            |> toValueOption
            |> ValueSome
        | ValueSome optFrom, ValueSome optTo ->
            let innerMapper =
                buildMapperDelegate struct (optFrom, tTo)
                |> fromFunc
                |> Expr.constant

            vOptionMap tTo innerMapper eFrom
            |> ValueSome

module ByteStream =

    let mapper (eFrom: Expr) (tTo: Type) =
        if eFrom.Type = typeof<byte array> && tTo = typeof<System.IO.MemoryStream>
        then
            tTo.GetConstructor([|eFrom.Type|])
            |> flip Expr.newObj [eFrom]
            |> ValueSome

        elif eFrom.Type = typeof<System.IO.MemoryStream> && tTo = typeof<byte array>
        then
            let dispose = 
                { isStatic = false
                  t = eFrom.Type
                  name = "Dispose"
                  args = []
                  returnType = voidType
                  methodGenerics = []
                  classGenerics = []
                  caseInsensitiveName = false }
                |> method
                |> Expr.call
                |> flip
                |> apply []

            let toArray =
                { isStatic = false
                  t = eFrom.Type
                  name = "ToArray"
                  args = []
                  returnType = tTo
                  methodGenerics = []
                  classGenerics = []
                  caseInsensitiveName = false }
                |> method
                |> Expr.call
                |> flip
                |> apply []

            eFrom
            |> Expr.cache (fun ms ->
                toArray ms
                |> Expr.cache (fun array ->
                    // TODO: dispose MS strategy
                    // [dispose ms; array]
                    [array]
                    |> Expr.block (ValueSome array.Type)))
            |> ValueSome
        else ValueNone

module NonMapper =
    let mapper (eFrom: Expr) (tTo: Type) =
        if eFrom.Type = tTo then ValueSome eFrom
        elif tTo.IsAssignableFrom eFrom.Type then Expr.convert tTo eFrom |> ValueSome
        else ValueNone

module EnumMapper =

    let mapper (eFrom: Expr) (tTo: Type): Expr voption =
        if not eFrom.Type.IsEnum || not tTo.IsEnum then ValueNone
        else

        eFrom
        |> Expr.convert typeof<int>
        |> Expr.convert tTo
        |> ValueSome

module FromConstMapper =

    let mapper (eFrom: Expr) (tTo: Type): Expr voption =

        let value = eFrom.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
        let isDynamoConst = value <> null && typeof<ConstantClass>.IsAssignableFrom(tTo)
        match struct (eFrom.Type.GetCustomAttribute<DynamodbTypeAttribute>(), isDynamoConst) with
        | null, _
        | _, false -> ValueNone
        | attr, _ when not attr.Const -> ValueNone
        | _, _ ->

            { isStatic = true
              t = typedefof<ConstantClass>
              name = "FindValue"
              args = [typeof<string>]
              returnType = tTo
              classGenerics = []
              methodGenerics = [tTo]
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [Expr.prop value.Name eFrom]
            |> ValueSome

module ToConstMapper =

    type private ConstTypeCache<'a> =
        static let cache =

            let valueProp = typeof<'a>.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
            let getValueProp v = valueProp.GetValue v :?> 'a

            typeof<'a>.GetFields(BindingFlags.Public ||| BindingFlags.Static)
            |> Seq.filter (fun x -> x.FieldType = typeof<'a>)
            |> Seq.map (fun x -> x.GetValue(null) :?> 'a |> tpl x.Name)
            |> Seq.append (
                typeof<'a>.GetProperties(BindingFlags.Public ||| BindingFlags.Static)
                |> Seq.filter (fun x -> x.PropertyType = typeof<'a>)
                |> Seq.map (fun x -> x.GetValue(null) :?> 'a |> tpl x.Name))
            |> Seq.fold (fun (s: Dictionary<_, _>) struct (k, v) ->
                s.Add(k, v)
                s) (Dictionary<string, 'a>(StringComparer.OrdinalIgnoreCase))
            :> IReadOnlyDictionary<_, _>

        static member parse x =
            if not (cache.ContainsKey x)
            then invalidOp $"Invalid const value \"{x}\" for type {typeof<'a>}"
            cache[x]

    let mapper (eFrom: Expr) (tTo: Type): Expr voption =

        let value = eFrom.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
        match struct (value, tTo.GetCustomAttribute<DynamodbTypeAttribute>()) with
        | null, _
        | _, null -> ValueNone
        | value, _ when value.PropertyType <> typeof<string> -> ValueNone
        | _, attr when not attr.Const -> ValueNone
        | value, _ ->
            { isStatic = true
              t = typedefof<ConstTypeCache<_>>
              name = nameof ConstTypeCache<_>.parse
              args = [typeof<string>]
              returnType = tTo
              classGenerics = [tTo]
              methodGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [Expr.prop value.Name eFrom]
            |> ValueSome

module EmptyTypeMapper =

    let mapper (eFrom: Expr) (tTo: Type): Expr voption =
        match tTo.GetCustomAttribute<DynamodbTypeAttribute>() with
        | null -> ValueNone
        | x when not x.Empty -> ValueNone
        | _ ->
            tTo.GetField("value", BindingFlags.Static ||| BindingFlags.Public)
            :> MemberInfo
            |> CSharp.toOption
            ?|? tTo.GetProperty("value", BindingFlags.Static ||| BindingFlags.Public)
            |> Expr.propStatic
            |> ValueSome

module KeyValuePairMapper =

    let mapper invokePropMap (eFrom: Expr) (tTo: Type) =

        ValueSome (tpl >>> sndT)
        <|? getKeyValuePairType eFrom.Type
        <|? getKeyValuePairType tTo
        ?>>= fun struct(toK, toV) ->
            Expr.cache (fun eFrom ->    
                let key: Expr =
                    Expr.prop "Key" eFrom
                    |> flip (invokePropMap "Key") toK
                let value =
                    Expr.prop "Value" eFrom
                    |> flip (invokePropMap "Value") toV

                typedefof<KeyValuePair<_,_>>
                    .MakeGenericType([|key.Type; value.Type|])
                    .GetConstructor([|key.Type; value.Type|])
                |> flip Expr.newObj [key; value]) eFrom |> ValueSome

module StringParseMapper =

    let mapper numberType (eFrom: Expr) (tTo: Type) =
        if eFrom.Type = numberType && tTo = typeof<string>
        then
            { isStatic = false
              t = eFrom.Type
              name = "ToString"
              args = []
              returnType = tTo
              methodGenerics = []
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip1To3 Expr.call eFrom []
            |> ValueSome

        elif eFrom.Type = typeof<string> && tTo = numberType
        then
            { isStatic = true
              t = tTo
              name = "Parse"
              args = [eFrom.Type]
              returnType = tTo
              methodGenerics = []
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [eFrom]
            |> ValueSome
        else ValueNone

module EnumerableMapper =

    type private Utils() =

        static member throwIfNull<'a> message =
            if not typeof<'a>.IsValueType && not typeof<'a>.IsInterface
            then id
            else fun (x: 'a) -> if box x <> null then x else invalidOp message

        static member wrapInThrowIfNull<'a, 'b> (message, f: 'a -> 'b) =
            let aStruct = not typeof<'a>.IsValueType && not typeof<'a>.IsInterface
            let bStruct = not typeof<'b>.IsValueType && not typeof<'b>.IsInterface

            if aStruct && bStruct then f
            else Utils.throwIfNull<'a> message >> f >> Utils.throwIfNull<'b> message

        static member seqMap<'a, 'b>(f: 'a -> 'b, x: 'a seq) = Seq.map f x

        static member fromDictionary<'a, 'b when 'a : comparison>(xs: KeyValuePair<'a, 'b> seq): Map<'a, 'b> =
            seq {
                for x in xs do
                    yield struct (x.Key, x.Value)
            } |> MapUtils.ofSeq

        static member toDictionary<'a, 'b when 'a : comparison>(xs: KeyValuePair<'a, 'b> seq): Dictionary<'a, 'b> =
            kvpToDictionary xs

        static member buildList<'a>(x: 'a seq) = List.ofSeq x

        static member buildArray<'a>(x: 'a seq) = Array.ofSeq x

        static member buildMList<'a>(x: 'a seq) = Enumerable.ToList x

    // todo: test
    let private wrapInThrowIfNull tFrom tTo message mapper =
        let fType = typedefof<_ -> _>.MakeGenericType([| tFrom; tTo |])
        let m =
            { isStatic = true
              t = typeof<Utils>
              name = nameof Utils.wrapInThrowIfNull
              args = [typeof<string>; fType]
              returnType = fType
              methodGenerics = [tFrom; tTo]
              classGenerics = []
              caseInsensitiveName = false }
            |> method

        m.Invoke(null, [|message; mapper|])

    let private seqMap (tTo: Type) (mapper: Expr) (expr: Expr) =
        let enmFromType = getEnumerableType expr.Type |> Maybe.expectSomeErr "Expected type %O to be a seq" expr.Type

        { isStatic = true
          t = typeof<Utils>
          name = nameof Utils.seqMap
          args = [mapper.Type; expr.Type]
          returnType = typedefof<_ seq>.MakeGenericType tTo
          methodGenerics = [enmFromType; tTo]
          classGenerics = []
          caseInsensitiveName = false }
        |> method
        |> flip Expr.callStatic [mapper; expr]

    let private fromSeq (tTo: Type) (seq: Expr) =
        let enmType = getEnumerableType tTo |> Maybe.expectSomeErr "Expected type %O to be a seq" tTo
        let m' =
            { isStatic = true
              t = typeof<Utils>
              name = ""
              args = [seq.Type]
              returnType = typeof<obj>
              methodGenerics = [enmType]
              classGenerics = []
              caseInsensitiveName = false }

        if not tTo.IsGenericType then invalidOp "Expected generic enumerable type"

        elif tTo.GetGenericTypeDefinition() = typedefof<_ seq>
        then ValueNone

        elif tTo.GetGenericTypeDefinition() = typedefof<Map<_, _>>
        then
               getKeyValuePairType enmType
               ?|> fun struct (k, v) ->
                   { m' with
                        name = nameof Utils.fromDictionary
                        returnType = typedefof<Map<_, _>>.MakeGenericType([|k; v|])
                        methodGenerics = [k; v] }

        elif tTo.GetGenericTypeDefinition() = typedefof<Dictionary<_, _>>
        then
               getKeyValuePairType enmType
               ?|> fun struct (k, v) ->
                   { m' with
                        name = nameof Utils.toDictionary
                        returnType = typedefof<Dictionary<_, _>>.MakeGenericType([|k; v|])
                        methodGenerics = [k; v] }

        elif tTo.GetGenericTypeDefinition() = typedefof<_ list>
        then { m' with name = nameof Utils.buildList; returnType = typedefof<_ list>.MakeGenericType([|enmType|]) } |> ValueSome

        elif tTo.GetGenericTypeDefinition() = typedefof<_ array>
        then { m' with name = nameof Utils.buildArray; returnType = typedefof<_ array>.MakeGenericType([|enmType|]) } |> ValueSome

        elif tTo.GetGenericTypeDefinition() = typedefof<MList<_>>
        then { m' with name = nameof Utils.buildMList; returnType = typedefof<MList<_>>.MakeGenericType([|enmType|]) } |> ValueSome

        else invalidOp $"UnExpected generic enumerable type {tTo}"
        ?|> method
        ?|> flip Expr.callStatic [seq]
        ?|? seq

    let mapper buildMapperDelegate (eFrom: Expr) (tTo: Type) =
        match struct (getEnumerableType eFrom.Type, getEnumerableType tTo) with
        | ValueNone, ValueNone
        | ValueSome _, ValueNone
        | ValueNone, ValueSome _ -> ValueNone
        | ValueSome enmFrom, ValueSome enmTo ->
            let innerMapper =
                buildMapperDelegate struct (enmFrom, enmTo)
                |> fromFunc
                |> wrapInThrowIfNull enmFrom enmTo $"Found null value in input or output to enumerable mapping function {eFrom} => {tTo}"
                |> Expr.constant

            seqMap enmTo innerMapper eFrom
            |> fromSeq tTo
            |> ValueSome

module ComplexObjectMapper =

    type private Utils() =
        static member isSome<'a> (x: 'a voption) = ValueOption.isSome x

    let private validatePropNames (t: Type) =
        let props =
            Array.append
            <| t.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
            <| t.GetProperties(BindingFlags.Instance ||| BindingFlags.NonPublic)
            |> Seq.map _.Name
            |> Collection.groupBy _.ToLower()
            |> Collection.mapSnd List.ofSeq
            |> Seq.filter (sndT >> List.length >> flip (>) 1)
            |> Collection.mapSnd (Str.join ",")
            |> Seq.map sndT
            |> Str.join "; "

        match props with
        | "" -> ()
        | duplicates -> invalidOp $"Duplicate property names (ignoring case) for type {t} are not supported ({props})"

    let private isMappableComplexType (t: Type) =
        t.Namespace.StartsWith("Amazon.") || t.Namespace.StartsWith("TestDynamo.")

    let private getBestConstructor (mapFromProperty: string -> PropertyInfo voption) (tTo: Type) =
        tTo.GetConstructors()
        |> Seq.map (fun c ->
            c.GetParameters()
            |> Seq.map (fun pm ->
                mapFromProperty pm.Name
                ?|> flip tpl pm)
            |> allOrNone
            ?|> tpl c)
        |> Maybe.traverse
        |> Seq.sortBy (sndT >> _.Length >> (*)-1)
        |> Collection.tryHead

    let private propMappings (mapFromProperty: string -> PropertyInfo voption) (toProperties: PropertyInfo seq) =

        toProperties
        |> Seq.filter _.CanWrite
        |> Seq.map (
            tplDouble
            >> mapFst (_.Name >> mapFromProperty)
            >> traverseTpl1)
        |> allOrNone

    let private compileConstructorMapParts (tFrom: Type) (tTo: Type) =

        let readProps =
            tFrom.GetProperties()
            |> Array.filter _.CanRead

        let readProp name =
            readProps
            |> Array.filter _.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
            |> Collection.tryHead

        getBestConstructor readProp tTo
        ?>>= fun constructor ->
            let propNotSetInConstructor =
                sndT constructor
                |> List.map fstT
                |> flip List.contains
                >> not

            tTo.GetProperties()
            |> Seq.filter _.CanWrite
            |> Seq.filter propNotSetInConstructor
            |> Seq.filter (_.Name >> IsSet.isIsSet >> not)
            |> propMappings readProp
            ?|> tpl constructor

    let private maybeInjectConstructorArg struct (eFrom: Expr, pFrom: PropertyInfo) (ctrArg: Expr) =
        IsSet.ifSetExpression struct (eFrom, pFrom)
        ?|> flip1To3 Expr.condition ctrArg (defaultOrNull ctrArg.Type |> Expr.constantT ctrArg.Type)
        ?|? ctrArg

    let private printConstructorArg struct (struct (fromProp, toParam), expr: Expr) =
        sprintf "Arg %s = %s: %A" toParam fromProp expr

    let private buildComplex tryInvokePropMap eFrom struct (c, ps) =
        Expr.maybeCache (fun eFrom ->
            ps
            |> Seq.map (
                fun struct (fromProp: PropertyInfo, toParam: ParameterInfo) ->
                    Expr.prop fromProp.Name eFrom
                    |> flip (tryInvokePropMap fromProp.Name) toParam.ParameterType
                    ?|> (
                        maybeInjectConstructorArg struct (eFrom, fromProp)
                        >> DebugUtils.debug printConstructorArg struct (fromProp.Name, toParam.Name)))
            |> allOrNone
            ?|> Expr.newObj c) eFrom

    let private ifOptionSomeExpression struct (eFrom: Expr, pFrom: PropertyInfo) =

        getValueOptionType pFrom.PropertyType
        ?|> fun opt ->
            { isStatic = true
              t = typeof<Utils>
              name = nameof Utils.isSome
              args = [pFrom.PropertyType]
              returnType = typeof<bool>
              classGenerics = []
              methodGenerics = [opt]
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [Expr.prop pFrom.Name eFrom]

    let private maybeAssign from assignment =

        [         
            IsSet.ifSetExpression from
            ?|> flip Expr.ifThen assignment

            ifOptionSomeExpression from
            ?|> flip Expr.ifThen assignment

            ValueSome assignment
        ]
        |> Maybe.traverse
        |> Seq.head

    /// <summary>Assumption that cachedTo can be used multiple times (i.e. is pre cached)</summary>
    let private buildPropMap tryInvokePropMap (eFrom: Expr) (cachedTo: Expr) struct (pFrom: PropertyInfo, pTo: PropertyInfo) =

        let to' = Expr.prop pTo.Name cachedTo

        let assignIsSet =
            IsSet.ifSetMember false true pTo
            ?>>= (
                Either.map1Of2 ValueSome
                >> Either.map2Of2 none
                >> Either.reduce)
            ?|> (
                _.Name
                >> flip Expr.prop cachedTo
                >> flip Expr.assign constTrue)

        Expr.maybeCache (fun eFrom ->
            Expr.prop pFrom.Name eFrom
            |> flip (tryInvokePropMap pFrom.Name) pTo.PropertyType
            ?|> (fun from' ->
                let assignValue = Expr.assign to' from'

                assignIsSet
                ?|> (
                    Seq.singleton
                    >> Collection.prepend assignValue
                    >> Expr.block ValueNone)
                ?|? assignValue
                |> maybeAssign struct (eFrom, pFrom))) eFrom

    let private assignProps tryInvokePropMap (eFrom: Expr) (eTo: Expr) props =

        Expr.maybeCache (fun eFrom ->
            Seq.map (flip (buildPropMap tryInvokePropMap eFrom)) props
            |> List.ofSeq
            |> function
                | [] -> ValueSome eTo
                | ps -> Expr.maybeMutate ps eTo) eFrom

    let private printConstructorMap =
        let valNoneString = asLazy "Constructor map: ValueNone"

        let valSomeString struct (state, struct (struct (constructor: ConstructorInfo, args), propSetters)) =
            let argStr =
                List.map (fun struct (prop: PropertyInfo, param: ParameterInfo) ->
                    sprintf "\n  %s: %s = %s: %s" param.Name (typeName param.ParameterType) prop.Name (typeName prop.PropertyType)) args
                |> Str.join ", "

            let setterStr =
                List.map (fun struct (fromProp: PropertyInfo, toProp: PropertyInfo) ->
                    sprintf "\n  %s: %s = %s: %s" toProp.Name (typeName toProp.PropertyType) fromProp.Name (typeName fromProp.PropertyType)) propSetters
                |> Str.join ", "

            let setterStr = if setterStr = "" then "" else sprintf "\n  %s" setterStr
            sprintf "Constructor map: %s(%s)%s" constructor.DeclaringType.Name argStr setterStr

        function
        | ValueNone -> DebugUtils.debug valNoneString () ValueNone
        | ValueSome x -> DebugUtils.debug valSomeString () x |> ValueSome

    let mapper tryInvokePropMap (eFrom: Expr) (tTo: Type): Expr voption =

        if isMappableComplexType eFrom.Type |> not || isMappableComplexType tTo |> not
        then ValueNone
        else

        validatePropNames eFrom.Type
        validatePropNames tTo

        Expr.maybeCache (fun eFrom ->
            compileConstructorMapParts eFrom.Type tTo
            |> printConstructorMap
            ?>>= (fun struct (constructor, propMappings) ->
                buildComplex tryInvokePropMap eFrom constructor
                ?>>= (flip (assignProps tryInvokePropMap eFrom) propMappings))) eFrom

let private nullProtect (eFrom: Expr) (tTo: Type) (mapper: Expr) =
    if not eFrom.Type.IsClass && not eFrom.Type.IsInterface
    then mapper
    else
        let isNull = Expr.equal eFrom (Expr.constantNull eFrom.Type)
        Expr.condition isNull (defaultOrNull tTo |> Expr.constantT tTo) mapper

let struct (amzDto, dtoAmz) =
    let addGenerics (t: Type) =
        match t.GetGenericArguments() with
        | [||] -> t
        // attr is hard coded into the code generator
        | [|x|] when x.Name = "attr" -> t.MakeGenericType [|typeof<AttributeValue>|]
        | _ -> invalidOp $"Unexpected generic type definition {t}"

    let allTypes =
        AppDomain.CurrentDomain.GetAssemblies()
        |> Seq.collect _.GetTypes()
        |> Array.ofSeq

    let getType name =
        allTypes
        |> Seq.filter (_.FullName >> (=)name)
        |> Collection.tryHead
        |> Maybe.expectSomeErr "Unable to find type: %s" name

    typeof<DynamodbTypeAttribute>.Assembly.GetTypes()
    |> Seq.map (fun x ->
        x.GetCustomAttribute<DynamodbTypeAttribute>()
        |> CSharp.toOption
        ?|> (_.Name >> getType >> tpl (addGenerics x)))
    |> Maybe.traverse
    |> Seq.fold (fun struct (amzDto: Dictionary<_, _>, dtoAmz: Dictionary<_, _>) struct (dto, amz) ->
        amzDto.Add(amz, dto)
        dtoAmz.Add(dto, amz)
        struct (amzDto, dtoAmz)) struct (Dictionary<_, _>(), Dictionary<_, _>())
    |> fun struct (x, y) -> struct (x :> IReadOnlyDictionary<_, _>, y :> IReadOnlyDictionary<_, _>)

type AttemptedMapperDelegate =
    { isValid: bool
      build: unit -> Delegate }

    with
    static member asOpt x =
        if x.isValid then x.build() |> ValueSome
        else ValueNone

module rec CompositeMappers =

    let private mapper (eFrom: Expr) (tTo: Type): Expr voption =

        [
            NonMapper.mapper
            EnumMapper.mapper
            FromConstMapper.mapper
            ToConstMapper.mapper
            EmptyTypeMapper.mapper
            KeyValuePairMapper.mapper invokePropMap
            ToAttributeValue.mapper invokePropMap
            FromAttributeValue.mapper invokePropMap
            // Any type can be added here which has a "T Parse(string x)" method
            StringParseMapper.mapper typeof<decimal>
            ByteStream.mapper
            VOption.mapper buildMapperDelegate invokeMap
            EnumerableMapper.mapper buildMapperDelegate
            ComplexObjectMapper.mapper tryInvokePropMap
        ]
        |> Seq.map (fun f -> f eFrom tTo)
        |> Maybe.traverse
        |> Seq.map (nullProtect eFrom tTo)
        |> Collection.tryHead

    let private tryBuildMapperDelegate' struct (tFrom: Type, tTo: Type): AttemptedMapperDelegate =
        try
            Expr.tryLambda1 tFrom (
                flip mapper tTo)
            ?|> (
                Expr.compile
                >> fun compiled -> { isValid = true; build = asLazy compiled })
            ?|?
                { isValid = false
                  build = fun _ -> MappingException(tFrom, tTo, null) |> raise }
        with
        | e -> MappingException(tFrom, tTo, e) |> raise

    let private tryBuildMapperDelegate =
        let k (x: struct (Type * Type)) = x
        memoize ValueNone k tryBuildMapperDelegate' >> sndT

    let private buildMapperDelegate =
        tryBuildMapperDelegate
        >> _.build()

    let private invokeMap' (eFrom: Expr) (tTo: Type) =
        { isStatic = true
          t = typedefof<MapperCache<_, _>>
          name = nameof MapperCache<_, _>.map
          args = [eFrom.Type]
          returnType = tTo
          methodGenerics = []
          classGenerics = [eFrom.Type; tTo]
          caseInsensitiveName = false }
        |> method
        |> flip Expr.callStatic [eFrom]

    let private invokeMap (eFrom: Expr) (tTo: Type) =
        try invokeMap' eFrom tTo
        with | e -> MappingException(eFrom.Type, tTo, e) |> raise

    let private invokePropMap propName (eFrom: Expr) (tTo: Type) =
        try invokeMap' eFrom tTo
        with
        | :? MappingException as e -> MappingException(propName, eFrom.Type, tTo, e) |> raise
        | e -> MappingException(propName, eFrom.Type, tTo, MappingException(eFrom.Type, tTo, e)) |> raise

    /// <summary>Returns None if types cannot be mapped</summary>
    let private canInvokeMap (tFrom: Type) (tTo: Type) =
        let m =
            { isStatic = true
              t = typedefof<MapperCache<_, _>>
              name = nameof MapperCache<_, _>.canMap
              args = []
              returnType = typeof<bool>
              methodGenerics = []
              classGenerics = [tFrom; tTo]
              caseInsensitiveName = false }
            |> method

        m.Invoke(null, [||]) :?> bool

    /// <summary>Returns None if types cannot be mapped</summary>
    let private tryInvokePropMap propName (eFrom: Expr) (tTo: Type) =
        if canInvokeMap eFrom.Type tTo
        then invokePropMap propName eFrom tTo |> ValueSome
        else ValueNone

    type MapperCache<'from, 't>() =

        static let mapperDelegate =
            tryBuildMapperDelegate struct (typeof<'from>, typeof<'t>)

        static let mapperOpt =
            mapperDelegate
            |> AttemptedMapperDelegate.asOpt
            ?|> fun x -> (x :?> System.Func<'from, 't>).Invoke

        static let mapper' =
            lazy((mapperDelegate.build() :?> System.Func<'from, 't>).Invoke)

        static member mapper = mapper'

        static member canMap () = mapperDelegate.isValid
        static member map x = mapper'.Value x

let private fromDtoObj' =

    let cache = Dictionary<Type, obj -> obj>()

    let map' obj =
        let fromType = obj.GetType()
        if cache.ContainsKey fromType |> not
        then
            let toType =
                if amzDto.ContainsKey fromType
                then amzDto[fromType]
                elif dtoAmz.ContainsKey fromType
                then dtoAmz[fromType]
                else invalidOp $"Cannot map type {fromType}"

            let param = Expr.param fromType
            let m =
                { isStatic = true
                  t = typedefof<CompositeMappers.MapperCache<_, _>>
                  name = nameof CompositeMappers.MapperCache<_, _>.map
                  args = [fromType]
                  returnType = toType
                  classGenerics = [fromType; toType]
                  methodGenerics = []
                  caseInsensitiveName = false }
                |> method
                |> Expr.callStatic

            let method = 
                Expr.lambda1 typeof<obj> (
                    Expr.convert fromType
                    >> List.singleton
                    >> m
                    >> Expr.convert typeof<obj>)
                |> Expr.compile
                |> fromFunc
                :?> (obj -> obj)

            cache.Add(fromType, method)

        cache[fromType] obj

    fun (x: obj) ->
        if x = null then null
        else lock cache (fun _ -> map' x)

let mapDto<'from, 't> = CompositeMappers.MapperCache<'from, 't>.mapper.Value
let fromDtoObj (x: obj) = fromDtoObj' x