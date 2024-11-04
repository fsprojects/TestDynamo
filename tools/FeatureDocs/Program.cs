// See https://aka.ms/new-console-template for more information

using Amazon.DynamoDBv2;
using Amazon.Runtime;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;

var done = Done();
var classes = typeof(IAmazonDynamoDB)
    .GetMethods(BindingFlags.Instance | BindingFlags.Public)

    //.Where(x => x.Name == "CreateTableAsync")

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
    .Select(x => (x.Item2.Item1 ? x.Item1 : null, x.Item2.Item2))
    .ToList();

var names = new List<string>();
var fullDescriptions = new List<string>();
foreach (var (name, fullDescription) in classes)
{
    if (name != null) names.Add(name);
    fullDescriptions.Add(fullDescription);
}

var index = "# Supported methods\n\n" + string.Join("\n", names.Select(d => $" * [{d}](#{d})")) + "\n\n";

Console.WriteLine(index + string.Join("\n\n", fullDescriptions));
File.WriteAllText("./Features.md", index + string.Join("\n\n", fullDescriptions));

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

string PrintSchema(string title, Dictionary<string, object> schema)
{
    var printed = schema
        .Where(x => x.Key != "__description")
        .Select(x => PrintSchemaItem(x, 4))
        .OrderBy(x => x.Item1)
        .ThenBy(x => x.Item2)
        .Select(x => x.Item2);
        
    var result = string.Join("\n", printed);
    if (result == "") return "";

    return $"\n\n### {title}\n\n{result}";
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

    if (description != "") description = $" - {description}";

    if (schema.Value is bool b && b)
        return ("1" + schema.Key, $" * ✅ {schema.Key}{description}");
        
    if (schema.Value is string)
        return ("1" + schema.Key, $" * ✅ {schema.Key}{description}");
        
    if (schema.Value is bool b2 && !b2)
        return ("2" + schema.Key, $" * {schema.Key}{description}");
        //return ("2" + schema.Key, $" * ❌ {schema.Key}{description}");
        
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

object ToObj(JsonElement el)
{
    if (el.ValueKind == JsonValueKind.True) return true;
    if (el.ValueKind == JsonValueKind.False) return false;
    if (el.ValueKind == JsonValueKind.String) return el.GetString()!;

    if (el.ValueKind != JsonValueKind.Object) throw new Exception();

    var d = new Dictionary<string, object>();
    foreach (var prop in el.EnumerateObject())
        d.Add(prop.Name, ToObj(prop.Value));

    return d;
}

Dictionary<string, object> Done()
{
    var replicaDescription = JsonSerializer.Serialize(new
    {
        __description = "Partial support. If an `InMemoryDynamoDbClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty",
        GlobalSecondaryIndexes = new
        {
            IndexName = true
        },
        RegionName = true,
        ReplicaStatus = true
    });

    var tableDescription = JsonSerializer.Serialize(new
    {
        CreationDateTime = true,
        ItemCount = true,
        TableSizeBytes = false,
        TableStatus = true,
        TableArn = true,
        TableId = true,
        StreamSpecification = true,
        LatestStreamArn = true,
        LatestStreamLabel = true,
        DeletionProtectionEnabled = true,
        TableName = true,
        LocalSecondaryIndexes = new
        {
            IndexName = true,
            IndexArn = true,
            ItemCount = true,
            KeySchema = true,
            Projection = true
        },
        GlobalSecondaryIndexes = new
        {
            Backfilling = true,
            IndexName = true,
            IndexArn = true,
            ItemCount = true,
            IndexStatus = true,
            KeySchema = true,
            Projection = true
        },
        GlobalTableVersion = true,
        AttributeDefinitions = true,
        KeySchema = true,
        Replicas = "{{replicaDescription}}"
    });

    var json = JsonSerializer.Serialize(new
    {
        CreateTableRequest = new
        {
            TableName = true,
            DeletionProtectionEnabled = true,
            KeySchema = true,
            AttributeDefinitions = true,
            StreamSpecification = true,
            GlobalSecondaryIndexes = new
            {
                IndexName = true,
                KeySchema = true,
                Projection = true
            },
            LocalSecondaryIndexes = new
            {
                IndexName = true,
                KeySchema = true,
                Projection = true
            }
        },
        CreateTableResponse = new
        {
            TableDescription = "{{tableDescription}}"
        },
        DeleteTableRequest = new
        {
            TableName = true
        },
        DeleteTableResponse = new
        {
            TableDescription = "{{tableDescription}}"
        },
        UpdateTableRequest = new
        {
            TableName = true,
            DeletionProtectionEnabled = true,
            AttributeDefinitions = true,
            StreamSpecification = true,
            GlobalSecondaryIndexUpdates = new
            {
                Create = new
                {
                    IndexName = true,
                    KeySchema = true
                },
                Delete = new
                {
                    IndexName = true
                }
            },
            ReplicaUpdates = new
            {
                Create = new
                {
                    RegionName = true
                },
                Delete = new
                {
                    RegionName = true
                }
            }
        },
        UpdateTableResponse = new
        {
            TableDescription = "{{tableDescription}}"
        },
        PutItemRequest = new
        {
            TableName = true,
            Item = true,
            ReturnValues = true,
            ConditionExpression = true,
            ExpressionAttributeNames = true,
            ExpressionAttributeValues = true
        },
        PutItemResponse = new
        {
            Attributes = true
        },
        ListGlobalTablesRequest = new
        {
            RegionName = true,
            Limit = true,
            ExclusiveStartGlobalTableName = true
        },
        DescribeGlobalTableRequest = new
        {
            GlobalTableName = true
        },
        DescribeGlobalTableResponse = new
        {
            GlobalTableDescription = new
            {
                CreationDateTime = true,
                GlobalTableName = true,
                GlobalTableArn = true,
                GlobalTableStatus = true,
                ReplicationGroup = "{{replicaDescription}}"
            }
        },
        ListGlobalTablesResponse = new
        {
            LastEvaluatedGlobalTableName = true,
            GlobalTables = new
            {
                GlobalTableName = true,
                ReplicationGroup = new
                {
                    RegionName = true
                }
            }
        },
        TransactGetItemsRequest = new
        {
            TransactItems = new
            {
                Get = new
                {
                    Key = true,
                    ExpressionAttributeNames = true,
                    TableName = true,
                    ProjectionExpression = true
                }
            }
        },
        TransactGetItemsResponse = new
        {
            Responses = new
            {
                Item = true
            }
        },
        DeleteItemRequest = new
        {
            Key = true,
            TableName = true,
            ConditionExpression = true,
            ExpressionAttributeNames = true,
            ExpressionAttributeValues = true,
            ReturnValues = true
        },
        ListTablesResponse = new 
        {
            TableNames = true,
            LastEvaluatedTableName = true
        },
        ListTablesRequest = new 
        {
            ExclusiveStartTableName = true,
            Limit = true
        },
        DeleteItemResponse = new
        {
            Attributes = true
        },
        DescribeTableRequest = new
        {
            TableName = true
        },
        DescribeTableResponse = new 
        {
            Table = "{{tableDescription}}"
        },
        TransactWriteItemsRequest = new
        {
            ClientRequestToken = true,
            TransactItems = new
            {
                Put = new
                {
                    ConditionExpression = true,
                    ExpressionAttributeNames = true,
                    ExpressionAttributeValues = true,
                    Item = true,
                    TableName = true
                },
                Delete = new
                {
                    ConditionExpression = true,
                    ExpressionAttributeNames = true,
                    ExpressionAttributeValues = true,
                    Key = true,
                    TableName = true
                },
                Update = new
                {
                    ConditionExpression = true,
                    ExpressionAttributeNames = true,
                    ExpressionAttributeValues = true,
                    Key = true,
                    UpdateExpression = true,
                    TableName = true
                },
                ConditionCheck = new
                {
                    ConditionExpression = true,
                    ExpressionAttributeNames = true,
                    ExpressionAttributeValues = true,
                    Key = true,
                    TableName = true
                }
            }
        },
        TransactWriteItemsResponse = new
        {
            ItemCollectionMetrics = new
            {
                ItemCollectionKey = true,
                SizeEstimateRangeGB = new
                {
                    __description = "Partial support. A constant value can be configured",
                }
            }
        },
        QueryRequest = new
        {
            TableName = true,
            IndexName = true,
            KeyConditionExpression = true,
            FilterExpression = true,
            Limit = true,
            ExpressionAttributeNames = true,
            ExpressionAttributeValues = true,
            ScanIndexForward = true,
            ExclusiveStartKey = true,
            ProjectionExpression = true,
            Select = true
        },
        QueryResponse = new
        {
            Items = true,
            Count = true,
            ScannedCount = true,
            LastEvaluatedKey = true
        },
        CreateGlobalTableRequest = new
        {
            GlobalTableName = true,
            ReplicationGroup = new
            {
                RegionName = true
            }
        },
        CreateGlobalTableResponse = new
        {
            GlobalTableDescription = new
            {
                CreationDateTime = true,
                GlobalTableName = true,
                GlobalTableArn = true,
                GlobalTableStatus = true,
                ReplicationGroup = "{{replicaDescription}}"
            }
        },
        UpdateGlobalTableRequest = new
        {
            GlobalTableName = true,
            ReplicaUpdates = new
            {
                Create = new
                {
                    RegionName = true
                },
                Delete = new
                {
                    RegionName = true
                }
            }
        },
        UpdateGlobalTableResponse = new
        {
            GlobalTableDescription = new
            {
                CreationDateTime = true,
                GlobalTableName = true,
                GlobalTableArn = true,
                GlobalTableStatus = true,
                ReplicationGroup = "{{replicaDescription}}"
            }
        },
        ScanRequest = new
        {
            TableName = true,
            IndexName = true,
            FilterExpression = true,
            Limit = true,
            ExpressionAttributeNames = true,
            ExpressionAttributeValues = true,
            ExclusiveStartKey = true,
            ProjectionExpression = true,
            Select = true
        },
        ScanResponse = new
        {
            Items = true,
            Count = true,
            ScannedCount = true,
            LastEvaluatedKey = true
        },
        GetItemRequest = new
        {
            TableName = true,
            ProjectionExpression = true,
            Key = true,
            ExpressionAttributeNames = true
        },
        GetItemResponse = new
        {
            Item = true,
            IsItemSet = true
        },
        BatchGetItemRequest = new
        {
            RequestItems = new
            {
                ConsistentRead = true,
                ExpressionAttributeNames = true,
                Keys = true,
                ProjectionExpression = true
            }
        },
        BatchGetItemResponse = new
        {
            Responses = true,
            UnprocessedKeys = new
            {
                ConsistentRead = true,
                ExpressionAttributeNames = true,
                Keys = true,
                ProjectionExpression = true
            }
        },
        BatchWriteItemRequest = new
        {
            RequestItems = new
            {
                PutRequest = new
                {
                    Item = true
                },
                DeleteRequest = new
                {
                    Key = true
                }
            }
        },
        BatchWriteItemResponse = new
        {
            UnprocessedItems = new
            {
                PutRequest = new
                {
                    Item = true
                },
                DeleteRequest = new
                {
                    Key = true
                }
            }
        },
        UpdateItemRequest = new
        {
            UpdateExpression = true,
            Key = true,
            AttributeUpdates = true,
            ConditionExpression = true,
            TableName = true,
            ReturnValues = true,
            ExpressionAttributeNames = true,
            ExpressionAttributeValues = true
        },
        UpdateItemResponse = new
        {
            Attributes = true
        }
    });

    return (Dictionary<string, object>)ToObj(
        JsonSerializer.Deserialize<JsonElement>(
            json
                .Replace(@"""{{tableDescription}}""", tableDescription)
                .Replace(@"""{{replicaDescription}}""", replicaDescription))!);
}

// return new Dictionary<string, object>
// {
//     ["CreateTableRequest"] = new Dictionary<string, object>
//     {
//         ["TableName"] = true,
//         ["DeletionProtectionEnabled"] = true,
//         ["KeySchema"] = true,
//         ["AttributeDefinitions"] = true,
//         ["GlobalSecondaryIndexes"] = new Dictionary<string, object>
//         {
//             ["IndexName"] = true,
//             ["KeySchema"] = true,
//             ["Projection"] = true
//         }
//     }
// };

