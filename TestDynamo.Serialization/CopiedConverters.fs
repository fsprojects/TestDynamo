module TestDynamo.Serialization.CopiedConverters

open System
open System.Collections.Concurrent
open System.Text.Json.Serialization
open System.Text.Json
open TestDynamo.GenericMapper.Utils.Reflection
open TestDynamo.Serialization.ConverterFramework
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

// Copied (modified slightly)
// from https://github.com/ShaneGH/SystexJson.FSharpConverters/blob/master/SystexJson.FSharpConverters/FSharpConverters.fs

module private Utils =
    let assertToken expected actual =
        match actual with
        | x when x = expected -> ()
        | _ -> sprintf "Invalid token type %A. Expecting %A." actual expected |> JsonException |> raise
        
    let implicitNulls (opts: JsonSerializerOptions) = opts.DefaultIgnoreCondition <> JsonIgnoreCondition.Never
open Utils

/// a mutable cache for converters, based on the Type
/// to convert and the type of factory that is requesting the converter
module ConverterCache =
    
    let private converterCache = ConcurrentDictionary<Type, JsonConverter>()
    let private canConvertCache = ConcurrentDictionary<(string * Type), bool>()
    
    type Cache =
        {
            canConvert: Type -> bool
            getConverter: Type -> JsonConverter
        }
       
    let build factoryType buildCanConvert (buildConverter: Type -> JsonConverter) =
        
        let canConvertWithFullKey (_, x) = buildCanConvert x
        let canConvert x = canConvertCache.GetOrAdd((factoryType, x), canConvertWithFullKey)
            
        let getConverter x = converterCache.GetOrAdd(x, buildConverter)
        
        {
            canConvert = canConvert
            getConverter = getConverter
        }
     
/// type converter and factory for options
/// None -> null, Some 'a -> 'a     
module Option =

    module private ConverterInternal =
        let readRefType<'a when 'a : null> (reader: byref<Utf8JsonReader>) (options: JsonSerializerOptions) =
            let result = JsonSerializer.Deserialize(&reader, typedefof<'a>, options)
            
            match result with
            | null -> ValueNone
            | x -> ValueSome (x :?> 'a)

        let writeRefType (writer: Utf8JsonWriter) value (options: JsonSerializerOptions) =
            match value with
            | ValueSome x -> JsonSerializer.Serialize(writer, x, options)
            | ValueNone -> JsonSerializer.Serialize(writer, null, options)
        
        let readValueType (reader: byref<Utf8JsonReader>) (options: JsonSerializerOptions) =
            let result = JsonSerializer.Deserialize(&reader, typeof<Nullable<'a>>, options) :?> Nullable<'a>
            match result.HasValue with
            | false -> ValueNone
            | true -> ValueSome result.Value

        let writeValueType<'a when 'a :> ValueType and 'a : struct and 'a : (new: Unit -> 'a)> (writer: Utf8JsonWriter) value (options: JsonSerializerOptions) =
            match value with
            | ValueSome (x: 'a) -> JsonSerializer.Serialize(writer, x, options)
            | ValueNone -> JsonSerializer.Serialize(writer, Nullable<'a>(), options)

    /// a JsonConverter for options when the inner value is a ref type
    type RefTypeConverter<'a when 'a : null>() =
        inherit JsonConverter<'a voption>()
        override _.Read(reader, _, options) = ConverterInternal.readRefType<'a> &reader options
        override _.Write(writer, value, options) = ConverterInternal.writeRefType writer value options

    /// a JsonConverter for options when the inner value is an value type
    type private ValueTypeConverter<'a when 'a :> ValueType and 'a : struct and 'a : (new: Unit -> 'a)>() =
        inherit JsonConverter<'a voption>()
        override _.Read(reader, _, options) = ConverterInternal.readValueType &reader options
        override _.Write(writer, value, options) = ConverterInternal.writeValueType writer value options
        
    let canConvertVal (typeToConvert: Type) =
        match typeToConvert.IsGenericType with
        | true -> typeToConvert.GetGenericTypeDefinition() = typedefof<ValueOption<_>>
        | false -> false
        
    let canConvertRef (typeToConvert: Type) =
        match typeToConvert.IsGenericType with
        | true -> typeToConvert.GetGenericTypeDefinition() = typedefof<Option<_>>
        | false -> false
            
    let createForValOpt (typ: Type) =
        let optionType = typ.GetGenericArguments().[0]
        match optionType.IsValueType with
        | true ->
            typedefof<ValueTypeConverter<_>>.MakeGenericType optionType
            |> Activator.CreateInstance
            :?> JsonConverter
        | false ->
            typedefof<RefTypeConverter<_>>.MakeGenericType optionType
            |> Activator.CreateInstance
            :?> JsonConverter
        
    let optToV<'a> (x: 'a option) =
        match x with
        | Some x -> ValueSome x
        | None -> ValueNone
        
    let vOptToR<'a> (x: 'a voption) =
        match x with
        | ValueSome x -> Some x
        | ValueNone -> None
        
    let optType (t: Type) =
        if t.IsGenericType
           && (
               t.GetGenericTypeDefinition() = typedefof<_ option>
               || t.GetGenericTypeDefinition() = typedefof<_ voption>)
        then ValueSome (t.GetGenericArguments()[0])
        else ValueNone
        
    type Cache<'a>() =
        static let opt': 'a option = None
        static let vopt': 'a voption = ValueNone
            
        static let surrogate' =
            let optType = typedefof<_ option>.MakeGenericType(typeof<'a>)
            let voptType = typedefof<_ voption>.MakeGenericType(typeof<'a>)
            let outputType = typedefof<SurrogateConverter<_, _>>.MakeGenericType([|optType; voptType|])
            let innerConverter = createForValOpt voptType
            
            Activator.CreateInstance(outputType, [|box innerConverter; optToV<'a>; vOptToR<'a>|])
            :?> JsonConverter
        
        static member optNone() = opt'
        static member voptNone() = vopt'
        static member surrogate() = surrogate'
        
    // returns none if this type is not an option or voption
    let optNone: Type -> obj voption =
        let none t vopt =
            ({ isStatic = true
               t = typedefof<Cache<_>>
               name = if vopt then nameof Cache<_>.voptNone else nameof Cache<_>.optNone
               args = []
               returnType = (if vopt then typedefof<_ voption> else typedefof<_ option>).MakeGenericType([|t|]) 
               classGenerics = [t]
               methodGenerics = []
               caseInsensitiveName = false }: MethodDescription)
            |> method
            |> _.Invoke(null, [||])
        
        let build (t: Type) =
            if not t.IsGenericType
            then ValueNone
            elif t.GetGenericTypeDefinition() = typedefof<_ option>
            then none (t.GetGenericArguments()[0]) false |> ValueSome 
            elif t.GetGenericTypeDefinition() = typedefof<_ voption>
            then none (t.GetGenericArguments()[0]) true |> ValueSome
            else ValueNone
        
        let k (t: Type) = t
        memoize ValueNone k build >> sndT
            
    let createForRefOpt (typ: Type) =
        
        ({ isStatic = true
           t = typedefof<Cache<_>>
           name = nameof Cache<_>.surrogate
           args = []
           returnType = typeof<JsonConverter> 
           classGenerics = [optType typ |> Maybe.expectSome]
           methodGenerics = []
           caseInsensitiveName = false }: MethodDescription)
        |> method
        |> _.Invoke(null, [||])
        :?> JsonConverter
            
    type ValueFactory() =
        inherit JsonConverterFactory()
        static let cache = ConverterCache.build "VOption" canConvertVal createForValOpt
        
        override _.CanConvert x = cache.canConvert x
        override _.CreateConverter (typ, _) = cache.getConverter typ
        
    //TODO: test
    type RefFactory() =
        inherit JsonConverterFactory()
        static let cache = ConverterCache.build "Option" canConvertRef createForRefOpt
        
        override _.CanConvert x = cache.canConvert x
        override _.CreateConverter (typ, _) = cache.getConverter typ
        
module RecordType =
    open System.Reflection
    open Microsoft.FSharp.Reflection
    
    module private ConverterInternal =
        
        let isNullable (t: Type) = t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<Nullable<_>>

        let getConstructorArgs (constructor: ConstructorInfo) (values: struct (string * obj) list) =
            constructor.GetParameters()
            |> Array.map (fun arg -> 
                values
                |> List.filter (fun struct (k, _) ->
                    k.Equals(arg.Name, StringComparison.InvariantCultureIgnoreCase))
                |> List.map sndT
                |> Collection.tryHead
                ?|> ValueSome
                ?|>? fun () -> Option.optNone arg.ParameterType
                ?|>? fun () -> sprintf "Could not find value for record type property \"%s\"" arg.Name |> JsonException |> raise)

        let buildRecordType (typ: Type) values =
            // TODO: remove reflection
            match typ.GetConstructors() with
            | [|constructor|] -> 
                getConstructorArgs constructor values
                |> constructor.Invoke
            | _ -> sprintf "Could not find constructor for record type %A" typ |> JsonException |> raise

        let getPropertyType (typ: Type) name =
            let props =
                typ.GetProperties()
                |> Seq.ofArray
                |> Seq.map (fun p -> struct (p.Name, p.PropertyType))

            let fields =
                typ.GetFields()
                |> Seq.ofArray
                |> Seq.map (fun p -> struct (p.Name, p.FieldType))

            Seq.concat [ props; fields ]
            |> Seq.filter (fun struct (n, _) -> n.Equals(name, StringComparison.InvariantCultureIgnoreCase))
            |> Seq.map (fun struct (_, v) -> v)
            |> Collection.tryHead

        let read (reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
            
            let mutable brk = false
            let mutable values = []
            while not brk do
                match reader.Read() with
                | true when reader.TokenType = JsonTokenType.EndObject ->
                    brk <- true
                    ()
                | true ->
                    assertToken JsonTokenType.PropertyName reader.TokenType
                    let key = reader.GetString()
                    let objType = 
                        getPropertyType typeToConvert key
                        |> ValueOption.defaultValue typedefof<obj>

                    let value = JsonSerializer.Deserialize(&reader, objType, options)
                    values <- struct (key, value)::values
                | false -> 
                    JsonException "Unexpected end of JSON sequence" |> raise

            buildRecordType typeToConvert (List.rev values)
        
    type Converter<'a>() =
        inherit JsonConverter<'a>()
        
        static let valType = typeof<'a>.IsValueType
        
        override _.Read(reader, typeToConvert, options) = ConverterInternal.read (&reader, typeToConvert, options) :?> 'a

        override _.Write(writer, value, options) =
            
            let implicitNulls = implicitNulls options
            
            // TODO: remove reflection
            match value with
            | x when not valType && box x = null -> writer.WriteNullValue()
            | x ->
                writer.WriteStartObject()
                x.GetType().GetFields()
                |> Array.map (fun f ->
                    match f.GetValue x, implicitNulls with
                    | null, true -> ()
                    | value, _ ->
                        writer.WritePropertyName f.Name
                        JsonSerializer.Serialize(writer, value, options)
                )
                |> ignore
                
                x.GetType().GetProperties()
                |> Array.map (fun p ->
                    match p.GetValue x, implicitNulls with
                    | null, true -> ()
                    | value, _ ->
                        writer.WritePropertyName p.Name
                        JsonSerializer.Serialize(writer, value, options)
                )
                |> ignore
                
                writer.WriteEndObject()
                
    module private FactoryInternal =
        
        let canConvert (typeToConvert: Type) = FSharpType.IsRecord typeToConvert
                
        let createForType (typ: Type) =
            let converterType = typedefof<Converter<_>>.MakeGenericType typ
            Activator.CreateInstance converterType :?> JsonConverter
            
    let private converterCache = ConverterCache.build "RecordType" FactoryInternal.canConvert FactoryInternal.createForType
        
    type Factory() =
        inherit JsonConverterFactory()

        override _.CanConvert typeToConvert = converterCache.canConvert typeToConvert
        
        override _.CreateConverter (typ, _) = converterCache.getConverter typ