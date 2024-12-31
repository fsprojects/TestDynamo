// See https://aka.ms/new-console-template for more information

using Amazon.DynamoDBv2;
using Amazon.Runtime;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

public static class FeatureDocs
{
    public static void Execute()
    {
        var done = Done();
        var classes = typeof(IAmazonDynamoDB)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)

            .Where(x => x.GetParameters().Length > 0)
            .GroupBy(x => x.Name)
            .Select(x => x
                .OrderBy(x => x.GetParameters().Length == 2 && x.GetParameters()[0].Name == "request" ? 0 : 1)
                .ThenBy(x => x.GetParameters().Length)
                .ThenBy(x => x.GetParameters()[0].Name == "request" ? 0 : 1)
                .First())
            .Select(x => new
            {
                x.Name,
                Inputs = DescribeRoot(
                    x.GetParameters()[0].ParameterType,
                    (Dictionary<string, object>?)done.GetValueOrDefault(RefineType(x.GetParameters()[0].ParameterType).Name)),
                Outputs = DescribeRoot(
                    x.ReturnType,
                    (Dictionary<string, object>?)done.GetValueOrDefault(RefineType(x.ReturnType).Name))
            })
            .Select(x => (x.Name, Print(x.Name, x.Inputs, x.Outputs)))
            .OrderBy(x => x.Item2.Item1 ? 0 : 1)
            .ThenBy(x => x.Item2.Item2)
            .Select(x => (x.Item2.Item1 ? x.Name : null, x.Item2.Item2))
            .ToList();

        var names = new List<string>();
        var fullDescriptions = new List<string>();
        foreach (var (name, fullDescription) in classes)
        {
            if (name != null) names.Add(name);
            fullDescriptions.Add(fullDescription);
        }

        var index = "# Supported methods\n\n" + string.Join("\n", names.Select(d => $" * [{d}](#{d})")) + "\n\n";
        var addendum = "\n\n" + string.Join("\n\n", new[]
        {
            "#### Replica Description Support\n\nPartial support. If an `AmazonDynamoDBClient` is built from a global database, then Replicas will be accurate. Otherwise this value will be empty"
        });

        var text = index + string.Join("\n\n", fullDescriptions) + addendum;

        Console.WriteLine(text);
        File.WriteAllText("./Features.md", text);

        (bool, string) Print(string name, Dictionary<string, object> inputs, Dictionary<string, object> outputs)
        {
            return !AnyTrue(inputs) && !AnyTrue(outputs)
                ? (false, $"### ❌ {name}")
                : (true, $"## {name}{PrintSchema("Inputs", inputs)}{PrintSchema("Outputs", outputs)}");
        }

        bool AnyTrue(Dictionary<string, object> vals)
        {
            foreach (var (k, v) in vals)
            {
                if (v is bool vb && vb) return true;
                if (v is Dictionary<string, object> vd && AnyTrue(vd)) return true;
            }

            return false;
        }

        bool IsObsolete(PropertyInfo p)
        {
            return p.GetCustomAttribute<ObsoleteAttribute>() != null;
        }

        // hack, the string should not have quotes, but not really important
        // for a throwaway app like this
        string RemoveQuotes(string x)
        {
            return Regex.Replace(x, @"(^"")|(""$)", "");
        }

        string PrintSchema(string title, Dictionary<string, object> schema)
        {
            // hack/duplicated logic
            var omitChildren = schema.TryGetValue("__omitChildren", out var omit) && true.Equals(omit);
            var description = schema.TryGetValue("__description", out var desc) && desc is string d
                ? $"\n\n * {RemoveQuotes(d)}\n" 
                : "";

            var printed = omitChildren
                ? Enumerable.Empty<string>()
                : schema
                    .Where(x => x.Key != "__description" && x.Key != "__omitChildren")
                    .Select(x => PrintSchemaItem(x, 4))
                    .OrderBy(x => x.Item1)
                    .ThenBy(x => x.Item2)
                    .Select(x => x.Item2);
                
            var result = string.Join("\n", printed);
            if (result == "" && description == "") return "";

            return $"\n\n### {title}{description}\n\n{result}";
        }

        string Indent(string str, int indent)
        {
            return new string(Enumerable.Range(0, indent).Select(_ => ' ').ToArray()) + str;
        }

        (string, string) PrintSchemaItem(KeyValuePair<string, object> schema, int indent)
        {
            var description = schema.Value switch
            {
                string str => str,
                Dictionary<string, object> d when d.ContainsKey("__description") => (string)d["__description"],
                _ => ""
            };
            
            var omitChildren = schema.Value switch
            {
                Dictionary<string, object> d when d.ContainsKey("__omitChildren") && d["__omitChildren"].Equals(true) => true,
                _ => false
            };

            Console.WriteLine(description);
            Console.WriteLine(RemoveQuotes(description));
            if (description != "") description = $" - {RemoveQuotes(description)}";

            if (schema.Value is bool b && b)
                return ("1" + schema.Key, $" * ✅ {schema.Key}{description}");
                
            if (schema.Value is string)
                return ("1" + schema.Key, $" * ✅ {schema.Key}{description}");
                
            if (schema.Value is bool b2 && !b2)
                return ("2" + schema.Key, $" * {schema.Key}{description}");
                
            if (omitChildren)
                return ("2" + schema.Key, $" * {schema.Key}{description}");

            if (schema.Value is not Dictionary<string, object> inner)
                throw new Exception();

            var printed = inner
                .Where(x => x.Key != "__description")
                .Select(x => PrintSchemaItem(x, indent + 4))
                .OrderBy(x => x.Item1)
                .ThenBy(x => x.Item2)
                .Select(x => Indent(x.Item2, indent))
                .Prepend($" * {schema.Key}{description}");
            
            return ("1" + schema.Key, string.Join("\n", printed));
        }

        Type RefineType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return RefineType(type.GenericTypeArguments[0]);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>))
                return RefineType(type.GenericTypeArguments[0]);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
                return RefineType(type.GenericTypeArguments[0]);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                return RefineType(type.GenericTypeArguments[1]);

            return type;
        }

        Dictionary<string, object> DescribeRoot(Type type, Dictionary<string, object>? done)
        {
            type = RefineType(type);
            var result = type
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => !IsObsolete(p))
                .Where(x => x.DeclaringType != typeof(AmazonWebServiceResponse))
                .Select(x => KeyValuePair.Create<string, object>(x.Name, false))
                .ToDictionary();

            var removeProps = result.Keys
                .Select(k => $"Is{k}Set")
                .Where(result.ContainsKey)
                .ToList();

            foreach (var k in removeProps)
            {
                result.Remove(k);
            }

            if (done != null)
            {
                foreach (var d in done)
                {
                    if (d.Value is bool vb && vb) 
                    {
                        result[d.Key] = true;
                    }
                    
                    if (d.Value is string vs) 
                    {
                        result[d.Key] = vs;
                    }

                    if (d.Value is Dictionary<string, object> vd)
                    {
                        result[d.Key] = Describe(type.GetProperty(d.Key)!.PropertyType, vd);
                    }
                }
            }

            return result;
        }

        object Describe(Type type, Dictionary<string, object>? done)
        {
            var result = DescribeRoot(type, done);
            if (result.All(x => x.Value is bool b && b))
                return true;
                
            if (result.All(x => x.Value is bool b && !b))
                return false;

            return result;
        }

        object ToObj(JsonNode el)
        {
            if (el.GetValueKind() == JsonValueKind.True) return true;
            if (el.GetValueKind() == JsonValueKind.False) return false;
            if (el.GetValueKind() == JsonValueKind.String) return el.ToJsonString()!;

            if (el.GetValueKind() != JsonValueKind.Object) throw new Exception();

            var d = new Dictionary<string, object>();
            foreach (var prop in el.AsObject())
                d.Add(prop.Key, ToObj(prop.Value ?? throw new Exception($"Null???? ({prop.Key})")));

            return d;
        }

        Dictionary<string, object> Done()
        {

            return (Dictionary<string, object>)ToObj(
                FeatureDescriptions.DescriptionsJson());
        }
    }
}

