module TestDynamo.Serialization.ConverterFramework

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Text.Json

 // use dictionary as map does not work, System.Type does not compare
type GenericConverterFactory(converters: IReadOnlyDictionary<Type, JsonSerializerOptions -> JsonConverter>) =
    inherit JsonConverterFactory()

    override this.CreateConverter(typeToConvert: Type, options: JsonSerializerOptions) =
        converters[typeToConvert] options

    override this.CanConvert(typeToConvert: Type) =
        converters.ContainsKey(typeToConvert)

type SurrogateConverter<'a, 'surrogate>(surrogateConverter: JsonConverter<'surrogate>, toSurrogate, fromSurrogate) =
    inherit System.Text.Json.Serialization.JsonConverter<'a>()

    override this.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) =
        surrogateConverter.Read(&reader, typeToConvert, options)
        |> fromSurrogate

    override this.Write(writer: Utf8JsonWriter, value: 'a, options: JsonSerializerOptions) =
        let surrogate = toSurrogate value
        surrogateConverter.Write(writer, surrogate, options)


