module TestDynamo.Serialization.Converters

open TestDynamo.Serialization.ConverterFramework
open TestDynamo.Serialization.CopiedConverters
open System
open System.Text.Json.Serialization
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Model
open TestDynamo.Utils
open System.Text.Json
open TestDynamo.Data
open TestDynamo.Data.Monads.Operators

module AwsConst =
    open System.Reflection
        
    type Converter<'a>() =
        inherit JsonConverter<'a>()
        
        static let values =
            typeof<'a>.GetProperties(BindingFlags.Static ||| BindingFlags.Public)
            |> Seq.filter (fun p -> p.PropertyType = typeof<'a>)
            |> Seq.map (fun p -> struct (p.Name, p.GetValue null :?> 'a))
            |> Lookup.create (ValueSome StringComparer.OrdinalIgnoreCase)

        override _.Read(reader, typeToConvert, options) =
            if reader.TokenType = JsonTokenType.String
            then 
                match reader.GetString() |> flip Lookup.tryGetValue values with
                | ValueSome x -> x
                | ValueNone -> ClientError.clientError $"Invalid value for const {typeof<'a>}"
            else JsonException $"Invalid value for const {typeof<'a>} {reader.TokenType}" |> raise

        override _.Write(writer, value, options) =
            writer.WriteStringValue (value.ToString())
                
    module private FactoryInternal =
        
        let canConvert (typeToConvert: Type) =
            match typeToConvert.GetCustomAttribute<DynamodbTypeAttribute>() with
            | null -> false
            | attr -> attr.Const
                
        let createForType (typ: Type) =
            let converterType = typedefof<Converter<_>>.MakeGenericType typ
            Activator.CreateInstance converterType :?> JsonConverter
            
    let private converterCache = ConverterCache.build "AwsConst" FactoryInternal.canConvert FactoryInternal.createForType
        
    type Factory() =
        inherit JsonConverterFactory()

        override _.CanConvert typeToConvert = converterCache.canConvert typeToConvert
        
        override _.CreateConverter (typ, _) = converterCache.getConverter typ

let private tplError = "Invalid JSON. Tuples are represented as arrays"

type private AttributeValueConverter(options: JsonSerializerOptions) =
    inherit System.Text.Json.Serialization.JsonConverter<AttributeValue>()

    static let err =
        [
           "Invalid attribute value."
           "For non null attributes expected an array of 2 elements."
           "Element 1 is the data type, element 2 is the data."
           """For null values expect the json string "null"."""
        ] |> Str.join " "

    static let hashMapType = function
        | "S" -> "SS"
        | "N" -> "NS"
        | "B" -> "BS"
        | x -> sprintf "Invalid attribute type %A" x |> invalidOp

    let setConverter = options.GetConverter(typeof<Set<AttributeValue>>) :?> JsonConverter<Set<AttributeValue>>
    let mapWriter = options.GetConverter(typeof<Map<string, AttributeValue>>) :?> JsonConverter<Map<string, AttributeValue>>
    let arrayWriter = options.GetConverter(typeof<AttributeValue array>) :?> JsonConverter<AttributeValue array>

    member private this.ReadNonNull(reader: byref<Utf8JsonReader>, opts: JsonSerializerOptions) =

        if reader.TokenType <> JsonTokenType.StartArray
        then JsonException err |> raise

        if reader.Read() |> not || reader.TokenType <> JsonTokenType.String
        then JsonException err |> raise
        let t = reader.GetString()

        if reader.Read() |> not || reader.TokenType = JsonTokenType.EndArray
        then JsonException err |> raise

        let result = 
            match t with
            | "S" -> reader.GetString() |> AttributeValue.String
            | "N" -> reader.GetDecimal() |> AttributeValue.Number
            | "B" -> reader.GetString() |> Convert.FromBase64String |> AttributeValue.Binary
            | "BOOL" -> reader.GetBoolean() |> AttributeValue.Boolean
            | "SS"
            | "NS"
            | "BS" -> setConverter.Read(&reader, typeof<Set<AttributeValue>>, opts) |> Set.toSeq |> AttributeSet.create |> AttributeValue.HashSet
            | "M" -> mapWriter.Read(&reader, typeof<Map<string, AttributeValue>>, opts) |> AttributeValue.HashMap
            | "L" -> arrayWriter.Read(&reader, typeof<AttributeValue array>, opts) |> AttributeListType.CompressedList |> AttributeValue.AttributeList
            | x -> JsonException $"Unknown attribute data type {x}" |> raise

        if reader.Read() |> not || reader.TokenType <> JsonTokenType.EndArray
        then JsonException err |> raise

        result

    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        if reader.TokenType = JsonTokenType.String && "null".Equals(reader.GetString(), StringComparison.OrdinalIgnoreCase)
        then AttributeValue.Null
        else this.ReadNonNull(&reader, options)

    override this.Write(writer: Utf8JsonWriter, value: AttributeValue, options: JsonSerializerOptions) =
        match value with
        | AttributeValue.Null -> writer.WriteStringValue "null"
        | v ->
            writer.WriteStartArray()
            match v with
            | AttributeValue.String x ->
                writer.WriteStringValue "S"
                writer.WriteStringValue x
            | AttributeValue.Number x ->
                writer.WriteStringValue "N"
                writer.WriteNumberValue x
            | AttributeValue.Binary x ->
                writer.WriteStringValue "B"
                Convert.ToBase64String x |> writer.WriteStringValue
            | AttributeValue.Boolean x ->
                writer.WriteStringValue "BOOL"
                writer.WriteBooleanValue x
            | AttributeValue.AttributeList xs ->
                writer.WriteStringValue "L"
                let xs = AttributeListType.asArray xs
                arrayWriter.Write(writer, xs, options)
            | AttributeValue.HashSet xs ->
                AttributeSet.getSetType xs |> AttributeType.describe |> hashMapType |> writer.WriteStringValue
                let xs = AttributeSet.asSet xs
                setConverter.Write(writer, xs, options)
            | AttributeValue.HashMap x ->
                writer.WriteStringValue "M"
                mapWriter.Write(writer, x, options)
            | AttributeValue.Null -> invalidOp "Handled in case before"
            writer.WriteEndArray()

let buildDatabaseIdConverter (opts: JsonSerializerOptions): JsonConverter<DatabaseId> =
    let ``to`` dbId = dbId.regionId
    let from dbId = {regionId = dbId}
    SurrogateConverter<DatabaseId, string>(opts.GetConverter(typeof<string>) :?> JsonConverter<string>, ``to``, from)

let buildAttributeTypeConverter (opts: JsonSerializerOptions): JsonConverter<AttributeType> =
    let ``to`` = AttributeType.describe
    let from attr = AttributeType.tryParse attr ?|>? fun _ -> $"Invalid attribute type {attr}" |> JsonException |> raise
    SurrogateConverter<AttributeType, string>(opts.GetConverter(typeof<string>) :?> JsonConverter<string>, ``to``, from)

let private converterDictionary = Dictionary<Type, JsonSerializerOptions -> JsonConverter>()
let private addConverter<'a> (f: JsonSerializerOptions -> JsonConverter<'a>) =
    let inline cast f x = f x :> JsonConverter
    converterDictionary.Add(typeof<'a>, cast f)

addConverter<AttributeValue> (fun x -> AttributeValueConverter x)
addConverter<AttributeType> buildAttributeTypeConverter
addConverter<DatabaseId> buildDatabaseIdConverter

let customConverters: JsonConverter list =
    [
        Option.ValueFactory()
        Option.RefFactory()
        GenericConverterFactory(converterDictionary)
        RecordType.Factory()
        AwsConst.Factory()
    ]

