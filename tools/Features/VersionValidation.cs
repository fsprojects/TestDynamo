
using System.Reflection;
using System.Text.RegularExpressions;
using Amazon.DynamoDBv2;

public static class VersionValidation
{
    public static void Validate()
    {
        var slnRoot = GetSlnRoot(new DirectoryInfo(System.IO.Directory.GetCurrentDirectory()));
        using var vFile = File.OpenRead($@"{slnRoot.FullName}\tests\testVersion.txt");
        using var vData = new StreamReader(vFile);

        var target = NormalizeVersion(Version.Parse(Regex.Replace(vData.ReadToEnd(), @"[^\d\.].*", "")));
        var actual = NormalizeVersion(typeof(AmazonDynamoDBClient).Assembly.GetName().Version ?? throw new Exception());

        if (actual != target)
            throw new Exception($"Expected version {target} got {actual}");
    }
    
    public static DirectoryInfo GetSlnRoot(DirectoryInfo cwd)
    {
        if (cwd.GetFiles().Any(f => f.Name == "TestDynamo.sln"))
            return cwd;

        if (cwd.Parent == null) throw new Exception("Could not find sln root");

        return GetSlnRoot(cwd.Parent);
    }

    private static Version NormalizeVersion(Version v)
    {
        return new Version(Math.Max(0, v.Major), Math.Max(0, v.Minor), Math.Max(0, v.Build), Math.Max(0, (int)v.MinorRevision));
    }
}