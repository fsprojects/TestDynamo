module TestDynamo.Serialization.Converters

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open TestDynamo.Model
open TestDynamo.Utils
open System.Text.Json
open TestDynamo.Data.Monads.Operators
         
 // use dictionary as map does not work, System.Type does not compare
type private GenericConverterFactory(converters: IReadOnlyDictionary<Type, JsonSerializerOptions -> JsonConverter>) =
    inherit JsonConverterFactory()
    
    override this.CreateConverter(typeToConvert: Type, options: JsonSerializerOptions) =
        converters[typeToConvert] options
     
    override this.CanConvert(typeToConvert: Type) =
        converters.ContainsKey(typeToConvert)
         
let private tplError = "Invalid JSON. Tuples are represented as arrays"
         
type private ValueTupleConverter<'a, 'b>(options: JsonSerializerOptions) =
    inherit System.Text.Json.Serialization.JsonConverter<struct ('a * 'b)>()
    
    let cA = options.GetConverter(typeof<'a>) :?> JsonConverter<'a>
    let cB = options.GetConverter(typeof<'b>) :?> JsonConverter<'b>
     
    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.StartArray
        then JsonException tplError |> raise
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let a = cA.Read(&reader, typeof<'a>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let b = cB.Read(&reader, typeof<'b>, options)
         
        if reader.Read() |> not || reader.TokenType <> JsonTokenType.EndArray
        then JsonException tplError |> raise
         
        struct (a, b)
         
    override this.Write(writer: Utf8JsonWriter, struct (a, b), options: JsonSerializerOptions) =
        writer.WriteStartArray()
        cA.Write(writer, a, options)
        cB.Write(writer, b, options)
        writer.WriteEndArray()
         
type private ValueTupleConverter<'a, 'b, 'c>(options: JsonSerializerOptions) =
    inherit System.Text.Json.Serialization.JsonConverter<struct ('a * 'b * 'c)>()
     
    let cA = options.GetConverter(typeof<'a>) :?> JsonConverter<'a>
    let cB = options.GetConverter(typeof<'b>) :?> JsonConverter<'b>
    let cC = options.GetConverter(typeof<'c>) :?> JsonConverter<'c>
     
    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.StartArray
        then JsonException tplError |> raise
         
        if reader.Read() |> not
        then JsonException tplError |> raise
        let a = cA.Read(&reader, typeof<'a>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let b = cB.Read(&reader, typeof<'b>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let c = cC.Read(&reader, typeof<'c>, options)
        
        if reader.Read() |> not || reader.TokenType <> JsonTokenType.EndArray
        then JsonException tplError |> raise
         
        struct (a, b, c)
         
    override this.Write(writer: Utf8JsonWriter, struct (a, b, c), options: JsonSerializerOptions) =
        writer.WriteStartArray()
        cA.Write(writer, a, options)
        cB.Write(writer, b, options)
        cC.Write(writer, c, options)
        writer.WriteEndArray()
         
type private ValueTupleConverter<'a, 'b, 'c, 'd>(options: JsonSerializerOptions) =
    inherit System.Text.Json.Serialization.JsonConverter<struct ('a * 'b * 'c * 'd)>()
     
    let cA = options.GetConverter(typeof<'a>) :?> JsonConverter<'a>
    let cB = options.GetConverter(typeof<'b>) :?> JsonConverter<'b>
    let cC = options.GetConverter(typeof<'c>) :?> JsonConverter<'c>
    let cD = options.GetConverter(typeof<'d>) :?> JsonConverter<'d>
     
    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.StartArray
        then JsonException tplError |> raise
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let a = cA.Read(&reader, typeof<'a>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let b = cB.Read(&reader, typeof<'b>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let c = cC.Read(&reader, typeof<'c>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let d = cD.Read(&reader, typeof<'d>, options)
         
        if reader.Read() |> not || reader.TokenType <> JsonTokenType.EndArray
        then JsonException tplError |> raise
         
        struct (a, b, c, d)
         
    override this.Write(writer: Utf8JsonWriter, struct (a, b, c, d), options: JsonSerializerOptions) =
        writer.WriteStartArray()
        cA.Write(writer, a, options)
        cB.Write(writer, b, options)
        cC.Write(writer, c, options)
        cD.Write(writer, d, options)
        writer.WriteEndArray()
         
type private ValueTupleConverter<'a, 'b, 'c, 'd, 'e>(options: JsonSerializerOptions) =
    inherit System.Text.Json.Serialization.JsonConverter<struct ('a * 'b * 'c * 'd * 'e)>()
     
    let cA = options.GetConverter(typeof<'a>) :?> JsonConverter<'a>
    let cB = options.GetConverter(typeof<'b>) :?> JsonConverter<'b>
    let cC = options.GetConverter(typeof<'c>) :?> JsonConverter<'c>
    let cD = options.GetConverter(typeof<'d>) :?> JsonConverter<'d>
    let cE = options.GetConverter(typeof<'e>) :?> JsonConverter<'e>
     
    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.StartArray
        then JsonException tplError |> raise
         
        if reader.Read() |> not
        then JsonException tplError |> raise
        let a = cA.Read(&reader, typeof<'a>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let b = cB.Read(&reader, typeof<'b>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let c = cC.Read(&reader, typeof<'c>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let d = cD.Read(&reader, typeof<'d>, options)
        
        if reader.Read() |> not
        then JsonException tplError |> raise
        let e = cE.Read(&reader, typeof<'e>, options)
         
        if reader.Read() |> not || reader.TokenType <> JsonTokenType.EndArray
        then JsonException tplError |> raise
         
        struct (a, b, c, d, e)
         
    override this.Write(writer: Utf8JsonWriter, struct (a, b, c, d, e), options: JsonSerializerOptions) =
        writer.WriteStartArray()
        cA.Write(writer, a, options)
        cB.Write(writer, b, options)
        cC.Write(writer, c, options)
        cD.Write(writer, d, options)
        cE.Write(writer, e, options)
        writer.WriteEndArray()
         
type private ValueTupleConverterFactory() =
    inherit JsonConverterFactory()
     
    static let vTupleAssembly = typeof<struct(int * int)>.Assembly
     
    override this.CreateConverter(typeToConvert: Type, options: JsonSerializerOptions) =
        match typeToConvert.GetGenericArguments() with
        | [|_; _|] & args ->
            Activator.CreateInstance(
               typeof<ValueTupleConverter<_, _>>.GetGenericTypeDefinition().MakeGenericType(args),
               [|box options|]) :?> JsonConverter 
        | [|_; _; _; _|] & args ->
            Activator.CreateInstance(
               typeof<ValueTupleConverter<_, _, _, _>>.GetGenericTypeDefinition().MakeGenericType(args),
               [|box options|]) :?> JsonConverter 
        | [|_; _; _|] & args ->
            Activator.CreateInstance(
               typeof<ValueTupleConverter<_, _, _, _>>.GetGenericTypeDefinition().MakeGenericType(args),
               [|box options|]) :?> JsonConverter 
        | [|_; _; _; _; _|] & args ->
            Activator.CreateInstance(
               typeof<ValueTupleConverter<_, _, _, _, _>>.GetGenericTypeDefinition().MakeGenericType(args),
               [|box options|]) :?> JsonConverter
        | xs -> $"Tuple {xs} not supported" |> JsonException |> raise
     
    override this.CanConvert(typeToConvert: Type) =
        typeToConvert.Assembly = vTupleAssembly
           && typeToConvert.FullName.StartsWith "System.ValueTuple`"

// not needed or tested, but might be useful later
// type private EitherConverter<'a, 'b>(options: JsonSerializerOptions) =
//     inherit System.Text.Json.Serialization.JsonConverter<Either<'a, 'b>>()
//          
//     static let err =
//         [
//            "Invalid attribute value"
//            "For non null attributes expected an array of 2 elements"
//            "Element 1 is the data type, element 2 is the data"
//            "For null values expect a json null literal"
//         ] |> Str.join ". "
//         
//     let innerConverter = ValueTupleConverter<int, 'a, 'b>(options)
//          
//     override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
//         match innerConverter.Read(&reader, innerConverter.Type, options) with
//         | struct (0, x, _) -> Either1 x
//         | struct (1, _, x) -> Either2 x
//         | _ -> invalidOp $"Invalid Either index"
//         
//     override this.Write(writer: Utf8JsonWriter, value: Either<'a, 'b>, options: JsonSerializerOptions) =
//         match value with
//         | Either1 x -> innerConverter.Write(writer, struct (0, x, Unchecked.defaultof<'b>), options)
//         | Either2 x -> innerConverter.Write(writer, struct (1, Unchecked.defaultof<'a>, x), options)
//          
// type private EitherConverterFactory() =
//     inherit JsonConverterFactory()
//      
//     static let vTupleAssembly = typeof<struct(int * int)>.Assembly
//      
//     override this.CreateConverter(typeToConvert: Type, options: JsonSerializerOptions) =
//         let args = typeToConvert.GetGenericArguments()
//         Activator.CreateInstance(
//            typeof<EitherConverter<_, _>>.GetGenericTypeDefinition().MakeGenericType(args),
//            [|box options|]) :?> JsonConverter 
//      
//     override this.CanConvert(typeToConvert: Type) =
//         typeToConvert.IsGenericType
//             && typeToConvert.GetGenericTypeDefinition() = typeof<Either<_, _>>.GetGenericTypeDefinition()

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
 
type private SurrogateConverter<'a, 'surrogate>(surrogateConverter: JsonConverter<'surrogate>, toSurrogate, fromSurrogate) =
    inherit System.Text.Json.Serialization.JsonConverter<'a>()
     
    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        surrogateConverter.Read(&reader, typeToConvert, options)
        |> fromSurrogate

    override this.Write(writer: Utf8JsonWriter, value: 'a, options: JsonSerializerOptions) =
        let surrogate = toSurrogate value
        surrogateConverter.Write(writer, surrogate, options)
     
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
        GenericConverterFactory(converterDictionary)
        ValueTupleConverterFactory()
    ]