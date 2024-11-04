namespace DynamoDbInMemory.Serialization

open System.Text.RegularExpressions
open System.Threading.Tasks
open DynamoDbInMemory.Api
open DynamoDbInMemory.Data
open DynamoDbInMemory.Data.BasicStructures
open DynamoDbInMemory.Model
open DynamoDbInMemory.Serialization.Data
open DynamoDbInMemory.Utils
open System.IO
open System.Text.Json
open System.Threading
open DynamoDbInMemory
open DynamoDbInMemory.Data.Monads.Operators
open Microsoft.Extensions.Logging

type VersionedData<'a>(
    data: 'a,
    version: string) =
    
    static member currentVersion = "1"
    
    member _.version = version
    member _.data = data

module private BaseSerializer =
    
    let options =
        let opts () =
            let options = JsonSerializerOptions()
            Converters.customConverters
            |> List.fold (options.Converters.Add |> asLazy) ()
            
            options.IncludeFields <- true
            options
        
        let options = opts()
        let indentOptions = opts()
        indentOptions.WriteIndented <- true
        
        fun indent -> if indent then indentOptions else options
    
    module Strings =
    
        let write indent data =
            let opts = options indent
            JsonSerializer.Serialize(VersionedData<_>(data, VersionedData<_>.currentVersion), opts)
            
        let read<'a> (json: string) =
            let opts = options false
            JsonSerializer.Deserialize<VersionedData<'a>>(json, opts).data
            
    module Streams =
        
        let write indent stream data =
            let opts = options indent
            JsonSerializer.Serialize(utf8Json = stream, options = opts, value = VersionedData<_>(data, VersionedData<_>.currentVersion))
            
        let read<'a> (stream: Stream) =
            let opts = options false
            JsonSerializer.Deserialize<VersionedData<'a>>(utf8Json = stream, options = opts).data
    
        let create indent data =
            let ms = new MemoryStream()
            write indent ms data
            ms.Position <- 0
            ms
            
    module StreamsAsync =
            
        let write indent stream c data =
            let opts = options indent
            JsonSerializer.SerializeAsync(utf8Json = stream, value = VersionedData<_>(data, VersionedData<_>.currentVersion), options = opts, cancellationToken = c)
            
        let read<'a> (stream: Stream) c =
            let opts = options false
            JsonSerializer.DeserializeAsync<VersionedData<'a>>(stream, opts, c) |%|> _.data
    
        let create indent c data =
            let ms = new MemoryStream()
            write indent ms c data 
            |> ValueTask
            |> Io.normalizeVt
            |%|> fun _ ->
                ms.Position <- 0
                ms

module DatabaseSerializer =
    
    let private defaultIndent = Option.defaultValue false
    let private defaultCancellation = Option.defaultValue CancellationToken.None
    
    let private toString indent =
        defaultIndent indent
        |> BaseSerializer.Strings.write
        
    let private toStream indent =
        defaultIndent indent
        |> BaseSerializer.Streams.write
        
    let private toStreamAsync indent c =
        let indent = defaultIndent indent
        let c = defaultCancellation c
        flip (BaseSerializer.StreamsAsync.write indent) c
        >>> ValueTask
        
    let private toFile indent file data =
        use file = File.OpenWrite file
        let indent = defaultIndent indent
        BaseSerializer.Streams.write indent file data
        
    let private toFileAsync indent c file data =
        ValueTask<_>(task = task {
            use file = File.OpenWrite file
            return! toStreamAsync indent c file data
        })
        
    let private createStream indent =
        defaultIndent indent
        |> BaseSerializer.Streams.create
        
    let private createStreamAsync indent c =
        let indent = defaultIndent indent
        let c = defaultCancellation c
        BaseSerializer.StreamsAsync.create indent c
    
    let private fromString = BaseSerializer.Strings.read
        
    let private fromStream = BaseSerializer.Streams.read
        
    let private fromStreamAsync c str =
        let c = defaultCancellation c
        BaseSerializer.StreamsAsync.read str c
        
    let private fromFile file =
        use file = File.OpenRead file
        BaseSerializer.Streams.read file
        
    let private fromFileAsync c file =
        ValueTask<_>(task = task {
            use file = File.OpenRead file
            return! fromStreamAsync c file
        })
    
    type Serializer<'a, 'ser>(toSerializable: bool -> 'a -> 'ser, fromSerializable: ILogger voption -> 'ser -> 'a) =
        
        let toSerializable = Option.defaultValue false >> toSerializable
        
        member _.ToString(data, ?schemaOnly, ?indent) = toSerializable schemaOnly data |> toString indent
        member _.FromString(json, ?logger) = fromString json |> fromSerializable (Maybe.fromRef logger)
        
        member _.ToStream(data, ?schemaOnly, ?indent) = toSerializable schemaOnly data |> createStream indent
        member _.ToStreamAsync(data, ?schemaOnly, ?indent, ?c) = toSerializable schemaOnly data |> createStreamAsync indent c
        
        member _.WriteToStream(data, stream, ?schemaOnly, ?indent) = toSerializable schemaOnly data |> toStream indent stream
        member _.WriteToStreamAsync(data, stream, ?schemaOnly, ?indent, ?c) = toSerializable schemaOnly data |> toStreamAsync indent c stream
        member _.ToFile(data, file, ?schemaOnly, ?indent) = toSerializable schemaOnly data |> toFile indent file
        member _.ToFileAsync(data, file, ?schemaOnly, ?indent, ?c) = toSerializable schemaOnly data |> toFileAsync indent c file
        
        member _.FromStream(json, ?logger) = fromStream json |> fromSerializable (Maybe.fromRef logger)
        member _.FromStreamAsync(json, ?logger, ?c) = fromStreamAsync c json |%|> fromSerializable (Maybe.fromRef logger)
        
        member _.FromFile(file, ?logger) = fromFile file |> fromSerializable (Maybe.fromRef logger)
        member _.FromFileAsync(file, ?logger, ?c) = fromFileAsync c file |%|> fromSerializable (Maybe.fromRef logger)
    
    let Database = Serializer<Api.Database, Version1.SerializableDatabase>(Version1.ToSerializable.Database.toSerializable [||], Version1.FromSerializable.Database.fromSerializable)
    let DistributedDatabase = Serializer<Api.DistributedDatabase, Version1.SerializableDistributedDatabase>(Version1.ToSerializable.DistributedDatabase.toSerializable, Version1.FromSerializable.DistributedDatabase.fromSerializable)