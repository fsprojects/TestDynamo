namespace TestDynamo.Serialization

open System.Runtime.InteropServices
open System.Text.Json.Nodes
open System.Threading.Tasks
open TestDynamo.Serialization.Data
open TestDynamo.Utils
open System.IO
open System.Text.Json
open System.Threading
open TestDynamo
open TestDynamo.Data.Monads.Operators
open Microsoft.Extensions.Logging

type VersionedData<'a>(
    data: 'a,
    version: string) =

    static member currentVersion = "1"

    member _.version = version
    member _.data = data

    static member getData (x: VersionedData<'a>) = x.data

module RawSerializers =

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
            JsonSerializer.Serialize(data, opts)

        let read<'a> (json: string) =
            let opts = options false
            JsonSerializer.Deserialize<'a>(json, opts)

        let internal writeVersioned indent data =
            VersionedData<_>(data, VersionedData<_>.currentVersion)
            |> write indent

        let internal readVersioned<'a> x =
            read<VersionedData<'a>> x |> VersionedData<_>.getData

    module Nodes =

        let write indent data =
            let opts = options indent
            JsonSerializer.SerializeToNode(data, opts)

        let read<'a> (json: JsonNode) =
            let opts = options false
            JsonSerializer.Deserialize<'a>(json, opts)

        let internal writeVersioned indent data =
            VersionedData<_>(data, VersionedData<_>.currentVersion)
            |> write indent

        let internal readVersioned<'a> x =
            read<VersionedData<'a>> x |> VersionedData<_>.getData

    module Streams =

        let internal writeVersioned indent stream data =
            let opts = options indent
            JsonSerializer.Serialize(utf8Json = stream, options = opts, value = VersionedData<_>(data, VersionedData<_>.currentVersion))

        let internal readVersioned<'a> (stream: Stream) =
            let opts = options false
            JsonSerializer.Deserialize<VersionedData<'a>>(utf8Json = stream, options = opts).data

        let internal createVersioned indent data =
            let ms = new MemoryStream()
            writeVersioned indent ms data
            ms.Position <- 0
            ms

    module StreamsAsync =

        let write indent (stream: Stream) c data =
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

    let private toString = RawSerializers.Strings.writeVersioned

    let private toStream = RawSerializers.Streams.writeVersioned

    let private toStreamAsync indent c =
        flip (RawSerializers.StreamsAsync.write indent) c
        >>> ValueTask

    let private toFile indent file data =
        use file = File.OpenWrite file
        RawSerializers.Streams.writeVersioned indent file data

    let private toFileAsync indent c file data =
        ValueTask<_>(task = task {
            use file = File.OpenWrite file
            return! toStreamAsync indent c file data
        })

    let private createStream = RawSerializers.Streams.createVersioned

    let private createStreamAsync =
        RawSerializers.StreamsAsync.create

    let private fromString = RawSerializers.Strings.readVersioned

    let private fromStream = RawSerializers.Streams.readVersioned

    let private fromStreamAsync c str =
        RawSerializers.StreamsAsync.read str c

    let private fromFile file =
        use file = File.OpenRead file
        RawSerializers.Streams.readVersioned file

    let private fromFileAsync c file =
        ValueTask<_>(task = task {
            use file = File.OpenRead file
            return! fromStreamAsync c file
        })

    type Serializer<'a, 'ser>(toSerializable: bool -> 'a -> 'ser, fromSerializable: ILogger voption -> 'ser -> 'a) =

        member _.ToString(
            data,
            [<Optional; DefaultParameterValue(false)>] schemaOnly,
            [<Optional; DefaultParameterValue(false)>] indent) = toSerializable schemaOnly data |> toString indent
        member _.FromString(json, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = fromString json |> fromSerializable (Maybe.Null.toOption logger)

        member _.ToStream(
            data,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent) = toSerializable schemaOnly data |> createStream indent
        member _.ToStreamAsync(
            data,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent,
            [<Optional; DefaultParameterValue(CancellationToken())>]c) = toSerializable schemaOnly data |> createStreamAsync indent c

        member _.WriteToStream(
            data,
            stream,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent) = toSerializable schemaOnly data |> toStream indent stream
        member _.WriteToStreamAsync(
            data,
            stream,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent,
            [<Optional; DefaultParameterValue(CancellationToken())>]c) = toSerializable schemaOnly data |> toStreamAsync indent c stream
        member _.ToFile(
            data,
            file,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent) = toSerializable schemaOnly data |> toFile indent file
        member _.ToFileAsync(
            data,
            file,
            [<Optional; DefaultParameterValue(false)>]schemaOnly,
            [<Optional; DefaultParameterValue(false)>]indent,
            [<Optional; DefaultParameterValue(CancellationToken())>]c) = toSerializable schemaOnly data |> toFileAsync indent c file

        member _.FromStream(json, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = fromStream json |> fromSerializable (Maybe.Null.toOption logger)
        member _.FromStreamAsync(
            json,
            [<Optional; DefaultParameterValue(null: ILogger)>] logger,
            [<Optional; DefaultParameterValue(CancellationToken())>] c) = fromStreamAsync c json |%|> fromSerializable (Maybe.Null.toOption logger)

        member _.FromFile(file, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = fromFile file |> fromSerializable (Maybe.Null.toOption logger)
        member _.FromFileAsync(
            file,
            [<Optional; DefaultParameterValue(null: ILogger)>] logger,
            [<Optional; DefaultParameterValue(CancellationToken())>] c) = fromFileAsync c file |%|> fromSerializable (Maybe.Null.toOption logger)

    module FSharp =
        let Database = Serializer<Api.FSharp.Database, Version1.SerializableDatabase>(Version1.ToSerializable.Database.toSerializable [||], Version1.FromSerializable.Database.fromSerializable)
        let GlobalDatabase = Serializer<Api.FSharp.GlobalDatabase, Version1.SerializableGlobalDatabase>(Version1.ToSerializable.GlobalDatabase.toSerializable, Version1.FromSerializable.GlobalDatabase.fromSerializable)

    let private dbToSerializable schemaOnly (db: Api.Database) = Version1.ToSerializable.Database.toSerializable [||] schemaOnly db.CoreDb
    let private dbFromSerializable logger (db: Version1.SerializableDatabase) =
        let fs = Version1.FromSerializable.Database.fromSerializable logger db
        new Api.Database(fs, disposeUnderlyingDatabase = true)
    let Database = Serializer<Api.Database, Version1.SerializableDatabase>(dbToSerializable, dbFromSerializable)

    let private globalDbToSerializable schemaOnly (db: Api.GlobalDatabase) = Version1.ToSerializable.GlobalDatabase.toSerializable schemaOnly db.CoreDb
    let private globalDbFromSerializable logger (db: Version1.SerializableGlobalDatabase) =
        let fs = Version1.FromSerializable.GlobalDatabase.fromSerializable logger db
        new Api.GlobalDatabase(fs, disposeUnderlyingDatabase = true)
    let GlobalDatabase = Serializer<Api.GlobalDatabase, Version1.SerializableGlobalDatabase>(globalDbToSerializable, globalDbFromSerializable)