module TestDynamo.GenericMapper.DtoMappers

open System
open System.IO
open System.Linq
open System.Reflection
open System.Text.RegularExpressions
open Microsoft.FSharp.Core
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Model
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
type IReadOnlyDictionary<'a, 'b> = System.Collections.Generic.IReadOnlyDictionary<'a, 'b>

let private none _ = ValueNone

type private DebugUtils() =

    // this env variable must be set before application startup
    static let traceDtoMapping =
#if DEBUG
        System.Environment.GetEnvironmentVariable "TRACE_DTO_MAPPING"
        |> Str.emptyStringToNull
        |> Maybe.Null.toOption
        ?|> (fun x -> Regex.IsMatch(x, @"^\s*(0|false|n|no|null)\s*$") |> not)
        ?|? false
#else
        false
#endif

    // keep private. Does not have any traceDtoMapping protection
    static member private debug'<'a> (f: 'a -> string, x: 'a) =
        Console.WriteLine($"DtoMap {f x}")
        x

    // keep private. Does not have any traceDtoMapping protection
    static member private debugExpression'<'a> (f: 'a -> string) (expr: Expr) =
        if (expr.Type <> typeof<'a>) then expr
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

    static member inline debug<'a, 's> (f: struct ('s * 'a) -> string) (s: 's) (x: 'a) =
#if DEBUG
        if traceDtoMapping then DebugUtils.debug'(f, struct (s, x)) |> sndT
        else x
#else
        x
#endif

    static member inline debugExpression<'a> (f: 'a -> string) (expr: Expr) =
#if DEBUG
        if traceDtoMapping then DebugUtils.debugExpression' f expr
        else expr
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
        
    let ifFalseSetToTrue =
        Expr.multiUseProp
        >>> (
            tplDouble
            >> mapFst Expr.not
            >> mapSnd (flip Expr.assign (Expr.constant true))
            >> uncurry Expr.ifThen)

module VOption =

    type private Utils<'a>() =
        
        static let nullableNull' = lazy(
            let t = typedefof<Nullable<_>>.MakeGenericType([|typeof<'a>|])
            Activator.CreateInstance t
            |> Expr.constantT t)
        
        static let optNone' =
            let n: 'a voption = ValueNone
            Expr.constantT typeof<'a voption> n
            
        static member nullableNull() = nullableNull'.Value
        static member optNone() = optNone'
    
    type private Utils() =

        static member valueOptionOrDefault<'a> (x: 'a voption): 'a =
            match x with | ValueSome x' -> x' | ValueNone -> Unchecked.defaultof<_>
            
        static member nullableOrDefault<'a when 'a: (new : unit -> 'a) and 'a: struct and 'a :> ValueType> (x: Nullable<'a>): 'a =
            if x.HasValue then x.Value else Unchecked.defaultof<_>
            
        static member nullableToOpt<'a when 'a: (new : unit -> 'a) and 'a: struct and 'a :> ValueType> (x: Nullable<'a>) =
            if x.HasValue then ValueSome x.Value
            else ValueNone
            
        static member optToNullable<'a when 'a: (new : unit -> 'a) and 'a: struct and 'a :> ValueType> (x: 'a voption) =
            match x with
            | ValueNone -> Unchecked.defaultof<Nullable<'a>>
            | ValueSome x -> Nullable<'a> x

        static member toValueOptionR<'a when 'a : null> (x: 'a) =
            match x with | null -> ValueNone | x -> ValueSome x

        static member toValueOptionV<'a> (x: 'a) = ValueSome x

        static member valueOptionMap<'a, 'b>(f: 'a -> 'b, x: 'a voption) = ValueOption.map f x
        //
        static member toNullable<'a when 'a: (new : unit -> 'a) and 'a: struct and 'a :> ValueType>(x: 'a) =
            Nullable<'a>(x)

    let private vOptionMap (mapper: Expr) (opt: Expr) =
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
                  if e.Type.IsValueType
                  then nameof Utils.toValueOptionV
                  else nameof Utils.toValueOptionR
              args = [e.Type]
              returnType = typedefof<_ voption>.MakeGenericType([|e.Type|])
              methodGenerics = [e.Type]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [e]

    let private toNullable (e: Expr) =
        match getNullableType e.Type with
        | ValueSome _ -> e
        | ValueNone ->
            { isStatic = true
              t = typeof<Utils>
              name = nameof Utils.toNullable
              args = [e.Type]
              returnType = typedefof<Nullable<_>>.MakeGenericType([|e.Type|])
              methodGenerics = [e.Type]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [e]
            
    let valueOptionOrDefault (option: Expr) =
        let opt = getValueOptionType option.Type |> Maybe.expectSomeErr "Expected type %O to be a voption" option.Type

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
            
    let nullableOrDefault (nullable: Expr) =
        let opt = getNullableType nullable.Type |> Maybe.expectSomeErr "Expected type %O to be nullable" nullable.Type

        { isStatic = true
          t = typeof<Utils>
          name = nameof Utils.nullableOrDefault
          args = [nullable.Type]
          returnType = opt
          methodGenerics = [opt]
          classGenerics = []
          caseInsensitiveName = false }
        |> method
        |> flip Expr.callStatic [nullable]
        
    let optOrNullable t =
        getValueOptionType t
        ?|> Either1
        ?|> ValueSome
        ?|? (getNullableType t ?|> Either2)
    
    let convertOptToNullable (expr: Expr) =
        match getValueOptionType expr.Type with
        | ValueNone -> invalidOp "An unexpected error has occurred"
        | ValueSome t ->
            { isStatic = true
              t = typeof<Utils>
              name = nameof Utils.optToNullable
              args = [expr.Type]
              returnType = typedefof<Nullable<_>>.MakeGenericType([|t|])
              methodGenerics = [t]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [expr]
    
    let convertNullableToOpt (expr: Expr) =
        match getNullableType expr.Type with
        | ValueNone -> invalidOp "An unexpected error has occurred"
        | ValueSome t ->
            { isStatic = true
              t = typeof<Utils>
              name = nameof Utils.nullableToOpt
              args = [expr.Type]
              returnType = typedefof<_ voption>.MakeGenericType([|t|])
              methodGenerics = [t]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> flip Expr.callStatic [expr]
    
    let ensureOpt (eFrom: Expr) =
        match getNullableType eFrom.Type with
        | ValueSome _ -> convertNullableToOpt eFrom
        | ValueNone -> toValueOption eFrom
        
    let private defaultVal name (t: Type) =
        { isStatic = true
          t = typedefof<Utils<_>>
          name = name
          args = []
          returnType = typeof<Expr>
          methodGenerics = []
          classGenerics = [t]
          caseInsensitiveName = false }
        |> method
        |> _.Invoke(null, [||])
        :?> Expr
        
    let optNone = 
        let inline k (t: Type) = t
        memoize ValueNone k (defaultVal (nameof Utils<_>.optNone)) >> sndT
        
    let nullableNull = 
        let inline k (t: Type) = t
        memoize ValueNone k (defaultVal (nameof Utils<_>.nullableNull)) >> sndT
        
    let private toValueOption' = asLazy toValueOption
    let private toNullable' = asLazy toNullable

    let rec mapper buildMapperDelegate invokeMap (eFrom: Expr) (tTo: Type) =
        match getNullableType eFrom.Type with
        | ValueSome _ -> mapper buildMapperDelegate invokeMap (ensureOpt eFrom) tTo
        | ValueNone ->
            match struct (getValueOptionType eFrom.Type, optOrNullable tTo) with
            | ValueNone, ValueNone -> ValueNone
            | ValueSome optFrom, ValueNone ->
                let innerMapper =
                    buildMapperDelegate struct (optFrom, tTo)
                    |> fromFunc
                    |> Expr.constant

                vOptionMap innerMapper eFrom
                |> valueOptionOrDefault
                |> ValueSome
            | ValueNone, ValueSome ``to`` ->
                
                let optTo = Either.reduce ``to``
                let mapperOut =
                    Either.map1Of2 toValueOption' ``to``
                    |> Either.map2Of2 toNullable'
                    |> Either.reduce
                    
                invokeMap eFrom optTo
                |> mapperOut
                |> ValueSome
            | ValueSome optFrom, ValueSome ``to`` ->
                
                let optTo = Either.reduce ``to``
                let mapperOut =
                    Either.map1Of2 (asLazy id) ``to``
                    |> Either.map2Of2 (asLazy convertOptToNullable)
                    |> Either.reduce
                    
                let innerMapper =
                    buildMapperDelegate struct (optFrom, optTo)
                    |> fromFunc
                    |> Expr.constant
                    
                vOptionMap innerMapper eFrom
                |> mapperOut
                |> ValueSome

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
            | ValueNone -> ClientError.clientError "AttributeValue object has no data"

    let private isNullAccessor (t: Type) =

        Expr.lambda1 t (fun arg ->

            [ t.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
              t.GetProperties(BindingFlags.Instance ||| BindingFlags.NonPublic) ]
            |> Seq.concat
            |> Seq.filter (fun x ->
                "Null".Equals(x.Name, StringComparison.OrdinalIgnoreCase)
                || "IsNull".Equals(x.Name, StringComparison.OrdinalIgnoreCase))
            |> Seq.filter (fun x -> x.PropertyType = typeof<bool> || x.PropertyType = typeof<Nullable<bool>>)
            |> Seq.filter _.CanRead
            |> Seq.map (fun x ->
                let getter = Expr.prop x.Name arg 
                if x.PropertyType = typeof<Nullable<bool>>
                then getter |> VOption.nullableOrDefault // converts null to false
                else getter)
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
              name = nameof Unchecked.defaultof<AttributeValueBuilder<_>>.attributeValue
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

        Expr.lambda1 inputType (fun mapFromParam ->

            let convertedParam =
                if mapToProp.Name = "NULL"
                // Constant true works because IsNULLSet will turn this on or off
                then invokePropMap mapFromProp (Expr.constant true) mapToProp.PropertyType
                else invokePropMap mapFromProp mapFromParam mapToProp.PropertyType

            let assignValue =
                Expr.prop mapToProp.Name
                >> flip Expr.assign convertedParam

            let assignIsSet =
                IsSet.ifSetMember false true mapToProp
                ?>>= Either.take1
                ?|> (piName >> IsSet.ifFalseSetToTrue)
                ?|? id

            mapToProp.DeclaringType.GetConstructors()
            |> Seq.filter (_.GetParameters() >> _.Length >> (=)0)
            |> Collection.tryHead
            |> Maybe.expectSomeErr "Expected type %A to have a parameterless constructor" mapToProp.DeclaringType
            |> flip Expr.newObj []
            |> Expr.mutate [ assignIsSet; assignValue; assignIsSet ])
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

module ByteStream =

    type private Util() =
        static member streamProcessor (s: MemoryStream) =
            use _ =
                if Settings.DisposeOfInputMemoryStreams
                then s :> IDisposable
                else Disposable.nothingToDispose
                
            s.ToArray()
            
    let mapper (eFrom: Expr) (tTo: Type) =
        if eFrom.Type = typeof<byte array> && tTo = typeof<System.IO.MemoryStream>
        then
            tTo.GetConstructor([|eFrom.Type|])
            |> flip Expr.newObj [eFrom]
            |> ValueSome

        elif eFrom.Type = typeof<System.IO.MemoryStream> && tTo = typeof<byte array>
        then 
            { isStatic = true
              t = typeof<Util>
              name = nameof Util.streamProcessor
              args = [typeof<System.IO.MemoryStream>]
              returnType = typeof<byte array>
              methodGenerics = []
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> Expr.callStatic
            |> apply [eFrom]
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
            |> Maybe.Null.toOption
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
            if typeof<'a>.IsValueType
            then id
            else fun (x: 'a) -> if box x <> null then x else invalidOp message

        static member wrapInThrowIfNull<'a, 'b> (message, f: 'a -> 'b) =
            let aStruct = typeof<'a>.IsValueType
            let bStruct = typeof<'b>.IsValueType

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
        
        static member buildIList<'a>(x: 'a seq) = Enumerable.ToList x :> IList<'a>
        
        static member buildIReadOnlyList<'a>(x: 'a seq) = Enumerable.ToList x :> IReadOnlyList<'a>
        
        static member buildICollection<'a>(x: 'a seq) = Enumerable.ToList x :> ICollection<'a>
        
        static member buildIReadOnlyCollection<'a>(x: 'a seq) = Enumerable.ToList x :> IReadOnlyCollection<'a>

    let private wrapInThrowIfNull tFrom tTo (message: string) mapper =
        
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

        if tTo.IsArray
        then { m' with name = nameof Utils.buildArray; returnType = tTo } |> ValueSome
        
        elif not tTo.IsGenericType
        then invalidOp "Expected generic enumerable type"

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
            
        elif tTo.GetGenericTypeDefinition() = typedefof<IList<_>>
        then { m' with name = nameof Utils.buildIList; returnType = typedefof<IList<_>>.MakeGenericType([|enmType|]) } |> ValueSome
            
        elif tTo.GetGenericTypeDefinition() = typedefof<IReadOnlyList<_>>
        then { m' with name = nameof Utils.buildIReadOnlyList; returnType = typedefof<IReadOnlyList<_>>.MakeGenericType([|enmType|]) } |> ValueSome
            
        elif tTo.GetGenericTypeDefinition() = typedefof<ICollection<_>>
        then { m' with name = nameof Utils.buildICollection; returnType = typedefof<ICollection<_>>.MakeGenericType([|enmType|]) } |> ValueSome
            
        elif tTo.GetGenericTypeDefinition() = typedefof<IReadOnlyCollection<_>>
        then { m' with name = nameof Utils.buildIReadOnlyCollection; returnType = typedefof<IReadOnlyCollection<_>>.MakeGenericType([|enmType|]) } |> ValueSome

        else invalidOp $"Unexpected generic enumerable type {tTo}"
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
                |> wrapInThrowIfNull enmFrom enmTo $"Found null value in input or output for enumerable mapping function {eFrom} => {tTo}"
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

    let private mapFromProperty' (mapFromProperty: string -> PropertyInfo voption) (param: ParameterInfo) =
        mapFromProperty param.Name
        ?|> (Either1 >> ValueSome)
        ?|>? (fun _ ->
            match struct (getValueOptionType param.ParameterType, getNullableType param.ParameterType) with
            | ValueSome t, _ -> VOption.optNone t |> Either2 |> ValueSome
            | _, ValueSome t -> VOption.nullableNull t |> Either2 |> ValueSome
            | _ -> ValueNone)
    
    let private foundBest = asLazy "Found best"
    let private getBestConstructor (mapFromProperty: string -> PropertyInfo voption) (tTo: Type) =
        
        let mapFromProperty = mapFromProperty' mapFromProperty
        
        tTo.GetConstructors()
        |> DebugUtils.debug (fun struct (_, x) -> $"{List.ofArray x}") ()
        |> Seq.map (fun c ->
            c.GetParameters()
            |> Seq.map (fun pm ->
                mapFromProperty pm
                ?|> flip tpl pm)
            |> allOrNone
            ?|> tpl c)
        |> Maybe.traverse
        |> Seq.sortBy (
            // sort by most properties which do not have default "Expr" expressions
            sndT
            >> Seq.map fstT
            >> Either.partition
            >> fstT
            >> List.length
            >> (*)-1)
        |> Collection.tryHead
        ?|> DebugUtils.debug foundBest ()
        
    let private debugProp struct (_, struct (x, y: PropertyInfo)) =
        $"{y.Name} {ValueOption.isSome x}"

    let private propMappings (mapFromProperty: string -> PropertyInfo voption) (toProperties: PropertyInfo seq) =

        toProperties
        |> Seq.filter _.CanWrite
        |> Seq.map (
            tplDouble
            >> mapFst (_.Name >> mapFromProperty)
            >> DebugUtils.debug debugProp ()
            >> traverseTpl1)
        |> Maybe.traverse
        
    let private debugConstructor =
        sndT
        >> fun (struct (ctr: ConstructorInfo, _) & x) -> $"{ctr.DeclaringType.Name}({ctr.GetParameters().Length})"
        |> flip DebugUtils.debug ()

    let private compileConstructorMapParts (tFrom: Type) (tTo: Type) =

        let readProps =
            tFrom.GetProperties()
            |> Array.filter _.CanRead

        let readProp name =
            readProps
            |> Array.filter _.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
            |> Collection.tryHead

        getBestConstructor readProp tTo
        ?|> (
            debugConstructor
            >> fun constructor ->
            
                let propNotSetInConstructor =
                    sndT constructor
                    |> List.map fstT
                    |> Either.partitionL
                    |> fstT
                    |> flip List.contains
                    >> not

                tTo.GetProperties()
                |> Seq.filter _.CanWrite
                |> Seq.filter propNotSetInConstructor
                |> Seq.filter (_.Name >> IsSet.isIsSet >> not)
                |> propMappings readProp
                |> List.ofSeq
                |> tpl constructor)

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
                fun struct (fromProp: Either<PropertyInfo, Expr>, toParam: ParameterInfo) ->
                    fromProp
                    |> Either.map1Of2 (fun fromProp -> Expr.prop fromProp.Name eFrom |> tpl fromProp.Name)
                    |> Either.map2Of2 (tpl "")
                    |> Either.reduce
                    |> uncurry tryInvokePropMap
                    <| toParam.ParameterType
                    ?|> (
                        fromProp
                        |> Either.ignore2
                        ?|> (tpl eFrom >> maybeInjectConstructorArg)
                        ?|? id))
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

    /// <param name="cachedTo">Assumption that cachedTo can be used multiple times (i.e. is pre cached)</param>
    let private buildPropMap tryInvokePropMap (eFrom: Expr) (cachedTo: Expr) struct (pFrom: PropertyInfo, pTo: PropertyInfo) =

        let assignIsSet =
            IsSet.ifSetMember false true pTo
            ?>>= Either.take1
            ?|> (piName >> IsSet.ifFalseSetToTrue)

        Expr.maybeCache (fun eFrom ->
            Expr.prop pFrom.Name eFrom
            |> flip (tryInvokePropMap pFrom.Name) pTo.PropertyType
            ?|> (fun from' ->
                Expr.prop pTo.Name cachedTo
                |> flip Expr.assign from'
                |> Expr.cache (fun assignValue ->
                    assignIsSet
                    ?|> (fun assign ->
                        [assign cachedTo; assignValue; assign cachedTo]
                        |> Expr.block ValueNone)
                    ?|? assignValue)
                |> maybeAssign struct (eFrom, pFrom))) eFrom

    let private assignProps tryInvokePropMap (eFrom: Expr) (eTo: Expr) props =

        Expr.maybeCache (fun eFrom ->
            Seq.map (flip (buildPropMap tryInvokePropMap eFrom)) props
            |> List.ofSeq
            |> function
                | [] -> ValueSome eTo
                | ps -> Expr.maybeMutate ps eTo) eFrom

    // good logging method, might be handy for debug in future
    let private printConstructorMap =
        
        let valSomeString (t: Type) struct (struct (constructor: ConstructorInfo, args), propSetters) =
            let argStr =
                List.map (fun struct (prop: Either<PropertyInfo, Expr>, param: ParameterInfo) ->
                    let setTo =
                        prop
                        |> Either.map1Of2 (fun prop -> sprintf "%s: %s" prop.Name (typeName prop.PropertyType))
                        |> Either.map2Of2 (fun prop -> sprintf "default %s" prop.Type.Name)
                        |> Either.reduce
                        
                    sprintf "\n  %s: %s = %s" param.Name (typeName param.ParameterType) setTo) args
                |> Str.join ", "
    
            let setterStr =
                List.map (fun struct (fromProp: PropertyInfo, toProp: PropertyInfo) ->
                    sprintf "\n  %s: %s = %s: %s" toProp.Name (typeName toProp.PropertyType) fromProp.Name (typeName fromProp.PropertyType)) propSetters
                |> Str.join ", "
    
            let setterStr = if setterStr = "" then "" else sprintf "\n  %s" setterStr
            sprintf "Constructor map: %s(%s)%s" constructor.DeclaringType.Name argStr setterStr
            
        let valString struct (t: Type, x) =
            match x with
            | ValueNone -> $"Constructor map ({t.Name}): ValueNone"
            | ValueSome x -> valSomeString t x
    
        DebugUtils.debug valString
        
    let constructionPassed = asLazy "Construction passed"
    let constructionFailed = asLazy "Construction failed"
    let private print someMsg noneMsg x =
        match x with
        | ValueNone & x -> DebugUtils.debug constructionFailed () x
        | ValueSome _ & x -> DebugUtils.debug constructionPassed () x
        
    let private printConstructorInvocation = print "Construction passed" "Construction failed"
    let private printAssignment = print "Assignment passed" "Assignment failed"

    let mapper tryInvokePropMap (eFrom: Expr) (tTo: Type): Expr voption =

        if isMappableComplexType eFrom.Type |> not || isMappableComplexType tTo |> not
        then ValueNone
        else

        validatePropNames eFrom.Type
        validatePropNames tTo

        Expr.maybeCache (fun eFrom ->
            compileConstructorMapParts eFrom.Type tTo
            |> printConstructorMap eFrom.Type
            ?>>= (fun struct (constructor, propMappings) ->
                buildComplex tryInvokePropMap eFrom constructor
                |> printConstructorInvocation
                ?>>= (flip (assignProps tryInvokePropMap eFrom) propMappings)
                |> printAssignment)) eFrom

let private nullProtect (eFrom: Expr) (tTo: Type) (mapper: Expr) =
    if eFrom.Type.IsValueType
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
        |> Seq.collect (fun x ->
            try
                x.GetTypes()
            with
            | :? ReflectionTypeLoadException as e -> e.Types |> Array.filter ((<>)null))
        |> Array.ofSeq

    let getType name =
        allTypes
        |> Seq.filter (_.FullName >> (=)name)
        |> Collection.tryHead

    typeof<DynamodbTypeAttribute>.Assembly.GetTypes()
    |> Seq.map (fun x ->
        x.GetCustomAttribute<DynamodbTypeAttribute>()
        |> Maybe.Null.toOption
        ?|> (fun attr -> attr.Name |> getType |> tpl (addGenerics x)))
    |> Maybe.traverse
    |> Seq.fold (fun struct (amzDto: Dictionary<_, _>, dtoAmz: Dictionary<_, _>) struct (dto, amz) ->
        match amz with
        | ValueSome amz -> amzDto.Add(amz, dto)
        | ValueNone -> ()
        
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
            Expr.tryLambda1 tFrom (flip mapper tTo)
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
                then dtoAmz[fromType] |> Maybe.expectSomeErr "Unable to find type: %A" fromType
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