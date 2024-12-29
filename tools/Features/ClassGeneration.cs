
using Amazon.DynamoDBv2;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.Runtime;
using System.Collections.Frozen;
using System.Reflection;
using System.Text.RegularExpressions;

public static class ClassGeneration
{
    public static readonly IReadOnlyDictionary<Type, string> Renames = new Dictionary<Type, string>
    {
        [typeof(Amazon.Runtime.Endpoints.Endpoint)] = "Endpoint2"
    };

    private static readonly IReadOnlyDictionary<Type, string> _requiredGenerics = new Dictionary<Type, string>
    {
        [typeof(Amazon.DynamoDBv2.Model.AttributeValue)] = "'attr",
        [typeof(DynamoDBEvent.AttributeValue)] = "'attr"
    };

    private static bool RequiresDefinition(Type t) =>
        (t.Assembly == typeof(IAmazonDynamoDB).Assembly 
            || t.Assembly == typeof(AmazonWebServiceRequest).Assembly
            || t.Assembly == typeof(DynamoDBEvent.DynamodbStreamRecord).Assembly)
        && !_requiredGenerics.ContainsKey(t);

    private static IEnumerable<Type> DeepTypes(Type t, HashSet<Type>? done = null)
    {
        done ??= new HashSet<Type>();
        if (!done.Add(t)) return [];
        
        return t
            .GetProperties()
            .SelectMany(x => RequiresDefinition(t)
                ? DeepTypes(x.PropertyType, done).Append(x.PropertyType)
                : [x.PropertyType])
            .Concat(t
                .GetGenericArguments()
                .SelectMany(gt => DeepTypes(gt, done)));
    }

    private static IEnumerable<string>? RequiredGenerics(Type t)
    {
        if (_requiredGenerics.TryGetValue(t, out var name))
            return null;

        var gens = DeepTypes(t)
            .Select(x => _requiredGenerics.TryGetValue(x, out var g) ? g : null!)
            .Where(x => x != null )
            .Distinct()
            .OrderBy(x => x)
            .GetEnumerator();

        return gens.MoveNext()
            ? Enumerate(gens)
            : null;

        static IEnumerable<string> Enumerate(IEnumerator<string> gens)
        {
            do
            {
                yield return gens.Current;
            } while (gens.MoveNext());
        }
    }
    
    public static IEnumerable<T> AtLeast1<T>(IEnumerable<T> xs, string errMessage)
    {
        var ok = false;
        foreach (var x in xs)
        {
            ok = true;
            yield return x;
        }

        if (!ok) throw new Exception(errMessage);
    }

    public static IEnumerable<string>? GetConstValues(Type type)
    {
        if (type == typeof(ConstantClass))
            return null;
            
        if (!type.IsAssignableTo(typeof(ConstantClass)))
            return null;

        var result = type
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(x => x.FieldType == type)
            .Select(x =>
            {
                dynamic val = x.GetValue(null)!;
                return (string)val.Value;
            });

        return AtLeast1(result, $"Expected at least 1 {type}");
    }

    public static void Execute()
    {
        var apiTypes = Execute(typeof(IAmazonDynamoDB)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .SelectMany(x => x.GetParameters().Select(x => x.ParameterType))
            .Concat(typeof(IAmazonDynamoDB)
                .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .Select(x => UnTaskify(x.ReturnType)))
            .Concat([typeof(DynamoDBEvent.DynamodbStreamRecord), typeof(DynamoDBEvent)]));

        var customParsers = string.Join("\n", new List<string>
        {
            @"let private emptyStringAsNone = function | ValueSome """" -> ValueNone | x -> x",
            @"let private emptyStringAsNull = function | """" -> null | x -> x"
        });

        var header = string.Join("\n", new List<string>
        {
            "// ############################################################################",
            "// #################### Auto generated code, do not modify ####################",
            "// ############################################################################",
            "",
            "open System",
            "open System.Runtime.CompilerServices",
            "open System.Net",
            "",
            "#if NETSTANDARD2_0",
            "type internal IsReadOnlyAttribute() = inherit System.Attribute()",
            "#endif",
            "",
            "[<AllowNullLiteral>]",
            "type DynamodbTypeAttribute(name: string, empty: bool, ``const``: bool) =",
            "    inherit Attribute()",
            "",
            "    member _.Name = name",
            "    member _.Empty = empty",
            "    member _.Const = ``const``",
            "",
            "    override _.ToString() = $\"{name} - empty: {empty}, const: {``const``}\""
        });

        var requestTypes = string.Join("\n\n", apiTypes
            .Where(x => !string.IsNullOrWhiteSpace(x.Value))
            .OrderBy(x => x.Key.Name)
            .Select(x => x.Value)
            .Where(x => !Regex.IsMatch(x, @"^\s+$")));

        var text = $"module rec TestDynamo.GeneratedCode.Dtos\n\n{header}\n\n{customParsers}\n\n{requestTypes}";
     //   Console.WriteLine(text);
     // C:\Dev\TestDynamo\TestDynamo\Client\DtoMappers.fs   
        File.WriteAllText(@".\TestDynamo.GeneratedCode\Dtos.fs", text);
    }

    private static Dictionary<Type, string> Execute(IEnumerable<Type> types)
    {
        var classes = FeatureDescriptions
            .DescriptionsJson()
            .Select(x => x.Key)
            .ToFrozenSet();

        return types
            .OrderBy(x => x.Name)
            .Aggregate(new Dictionary<Type, string>(), (s, x) => EmitClassSchema(x, s, true));
    }

    public static Type UnTaskify(Type t)
    {
        if (!t.IsGenericType) return t;
        if (t.GetGenericTypeDefinition() == typeof(Task<>)
            || t.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            return t.GetGenericArguments()[0];
        }

        return t;
    }

    public static string _TypeName(Type t) 
    {
        if (_requiredGenerics.TryGetValue(t, out var g)) return g;

        var name = Renames.TryGetValue(t, out var n) ? n : t.Name;
        var generics = RequiredGenerics(t);
        return generics == null
            ? name
            : $"{name}<{string.Join(", ", generics)}>";
    }

    public static string TypeName(Type t) 
    {
        if (!t.IsGenericType) return _TypeName(t);

        var name = t.Name.Split("`")[0];
        name = name switch
        {
            "Dictionary" => "Map<{0}, {1}>",
            "IDictionary" => "Map<{0}, {1}>",
            "List" => "{0} array",
            "IList" => "{0} array",
            var x => x
        };

        var ts = t.GetGenericArguments().Select(TypeName);
        return string.Format(name, ts.ToArray());
    }

    public static Dictionary<Type, string> EmitEnumSchema(Type t, Dictionary<Type, string> emissions)
    {
        if (emissions.ContainsKey(t)) return emissions;
        if (t.Assembly != typeof(IAmazonDynamoDB).Assembly && t.Assembly != typeof(AmazonWebServiceRequest).Assembly) return emissions;

        var stringBuilder1 = new List<string>
        {
            $"[<DynamodbType(\"{t.FullName}\", false, false)>]",
            $"type {TypeName(t)} ="
        };

        var done = new List<string>();
        foreach (var f in Enum.GetValues(t))
        {
            var name = f.ToString()!;
            if (done.Contains(name)) continue;
            done.Add(name);

            var val = Convert.ChangeType(f, typeof(int));
            stringBuilder1.Add($"    | {name} = {val}");
        }

        emissions.Add(t, string.Join("\n", stringBuilder1));
        return emissions;
    }

    private static Type? GetOptionType(Type t)
    {
        return t.FullName?.StartsWith("Microsoft.FSharp.Core.FSharpValueOption") ?? false
            ? t.GetGenericArguments()[0]
            : null;
    }

    private static Type? GetNullableType(Type t)
    {
        return t.FullName?.StartsWith("System.Nullable") ?? false
            ? t.GetGenericArguments()[0]
            : null;
    }

    public static Dictionary<Type, string> EmitClassSchema(Type t, Dictionary<Type, string> emissions, bool forceRefType)
    {
        if (t.IsEnum) return EmitEnumSchema(t, emissions);

        if (emissions.ContainsKey(t)) return emissions;

        Console.WriteLine($"Executing {t}");
        var props = t
            .GetProperties()
            .Where(x => 
                !x.Name.StartsWith("IsSet", StringComparison.OrdinalIgnoreCase) 
                && !x.Name.EndsWith("IsSet", StringComparison.OrdinalIgnoreCase)
                && (!x.Name.StartsWith("Is", StringComparison.OrdinalIgnoreCase) || !x.Name.EndsWith("Set", StringComparison.OrdinalIgnoreCase)))
            .Select(x => (x.Name, x.PropertyType))
            .ToArray();

        // add a placeholder to stop infinitel loop
        emissions.Add(t, "");
        emissions = t
            .GetGenericArguments()
            .Concat(props.Select(x => x.PropertyType))
            .Concat(t.BaseType != null && t.BaseType != typeof(object) ? [t.BaseType] : Array.Empty<Type>())
            .Aggregate(emissions, (s, x) => EmitClassSchema(x, s, false));

        // after any child generic args are done, decide if this type needs to be done
        if (!RequiresDefinition(t)) return emissions;

        var typeName = TypeName(t);
        var consts = GetConstValues(t);
        var isConst = consts != null;
        var isConstFlag = isConst.ToString().ToLower();

        var isEmptyObj = props.Length == 0;
        var isEmptyObjFlag = isEmptyObj.ToString().ToLower();

        var structAttr = !isEmptyObj && !isConst && props.Length <= 2 && !forceRefType ? "; Struct; IsReadOnly" : "";
        var stringBuilder = new List<string>
        {
            $"[<DynamodbType(\"{t.FullName}\", {isEmptyObjFlag}, {isConstFlag}){structAttr}>]"
        };
        
        if (isEmptyObj)
        {
            stringBuilder.Add($"type {typeName} private () =");
            stringBuilder.Add($"    static member value = {typeName}()");
        }
        else if (consts != null)
        {
            stringBuilder.Add($"type {typeName} private (value: string) =");
            stringBuilder.Add($"    member _.Value = value");

            foreach (var val in consts)
            {
                // consts
                var code = $"{typeName}(\"{val}\")";
                stringBuilder.Add($"    static member {val} = {code}");
            }

            stringBuilder.Add($"    override this.ToString() = this.Value");
        }
        else
        {
            stringBuilder.Add($"type {typeName} =");

            var first = true;
            foreach (var prop in props)
            {
                var open = first ? "{" : " ";
                first = false;
                
                var vopt = GetOptionType(prop.PropertyType);
                var nullable = GetNullableType(prop.PropertyType);
                if (nullable != null && GetOptionType(nullable) != null)
                    // ValueOption<int?>
                    throw new Exception($"Unsupported type {prop.PropertyType}");

                var voption = vopt == null || nullable != null ? " voption" : "";
                stringBuilder.Add($"    {open} {prop.Name}: {TypeName(nullable ?? prop.PropertyType)}{voption}");
            }
            
            stringBuilder[^1] = $"{stringBuilder[^1]} }}";
        }

        emissions[t] = string.Join("\n", stringBuilder);
        return emissions;
    }

    private static string ApplyParser(Type t, string name, bool noneify)
    {
        if (t == typeof(string)) return $"{(noneify ? "emptyStringAsNone" : "emptyStringAsNull")} {name}";
        return name;
    }
}

