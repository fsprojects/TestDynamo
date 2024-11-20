// See https://aka.ms/new-console-template for more information

using Amazon.DynamoDBv2;
using System.Collections.Frozen;
using System.Reflection;

public static class ClassGeneration
{
    public static void Execute()
    {
        var classes = FeatureDescriptions
            .DescriptionsJson()
            .Select(x => x.Key)
            .ToFrozenSet();

        var apiTypes = typeof(IAmazonDynamoDB)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .SelectMany(x => x.GetParameters().Select(x => x.ParameterType).Prepend(x.ReturnType))
            .Where(x => classes.Contains(x.Name))
            .Aggregate(new Dictionary<Type, string>(), (s, x) => EmitClassSchema(x, s));

        var customParsers = string.Join("\n", new List<string>
        {
            @"let private emptyStringAsNone = function | ValueSome """" -> ValueNone | x -> x"
        });

        var opens = string.Join("\n", new List<string>
        {
            "open System",
            "open System.IO",
            "open System.Collections.Generic"
        });

        var types = string.Join("\n\n", apiTypes.Values);
        var text = $"module rec TheModule\n\n{opens}\n\n{customParsers}\n\n{types}";
        Console.WriteLine(text);
        File.WriteAllText(@".\TestDynamo.GeneratedCode\Dtos.fs", text);
    }

    public static string TypeName(Type t) 
    {
        if (!t.IsGenericType) return t.Name;

        var name = t.Name.Split("`")[0];
        var ts = t.GetGenericArguments().Select(TypeName);
        return $"{name}<{string.Join(", ", ts)}>";
    }

    public static Dictionary<Type, string> EmitClassSchema(Type t, Dictionary<Type, string> emissions)
    {
        if (emissions.ContainsKey(t)) return emissions;

        // add a placeholder to stop infinitel loop
        var props = t
            .GetProperties()
            .Where(x => !x.Name.StartsWith("Is") || !x.Name.EndsWith("Set"))
            .ToArray();

        emissions.Add(t, "");
        emissions = t
            .GetGenericArguments()
            .Concat(props.Select(x => x.PropertyType))
            .Aggregate(emissions, (s, x) => EmitClassSchema(x, s));

        if (t.Assembly != typeof(IAmazonDynamoDB).Assembly) return emissions;

        var stringBuilder1 = new List<string>();
        var stringBuilder2 = new List<string>();
        stringBuilder1.Add($"type {t.Name}() =");
        stringBuilder1.Add($"");

        foreach (var prop in props)
        {
            stringBuilder1.Add($"    let mutable _{prop.Name}: {TypeName(prop.PropertyType)} voption = ValueNone");
            stringBuilder2.Add($"    member _.{prop.Name}");
            stringBuilder2.Add($"       with get () = _{prop.Name}");
            stringBuilder2.Add($"       and set v = _{prop.Name} <- {ApplyParser(prop.PropertyType, "v")}");
        }

        if (stringBuilder2.Count == 0)
            stringBuilder2.Add("    member _.Dummy() = ()");

        stringBuilder1.Add("");
        emissions[t] = string.Join("\n", stringBuilder1.Concat(stringBuilder2));
        return emissions;
    }

    private static string ApplyParser(Type t, string name)
    {
        if (t == typeof(string)) return $"emptyStringAsNone {name}";
        return name;
    }
}

