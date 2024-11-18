open System
open System.Text
open System.Threading
open Amazon.DynamoDBv2
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open BenchmarkDotNet.Running
open BenchmarkDotNet.Toolchains.InProcess.Emit
open BenchmarkDotNet.Toolchains.InProcess.NoEmit
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
open Xunit.Runners
open Tests.Utils
open TestDynamo.Client.ItemMapper

module Program =
    type AntiVirusFriendlyConfig() =
        inherit ManualConfig()
        
        do
            base.AddJob(Job.MediumRun
                .WithToolchain(InProcessEmitToolchain.Instance)) |> ignore;

    [<MemoryDiagnoser>]
    // [<InliningDiagnoser>]
    // [<TailCallDiagnoser>]
    // [<EtwProfiler>]
    // [<ConcurrencyVisualizerProfiler>]
    // [<NativeMemoryProfiler>]
    // [<ThreadingDiagnoser>]
    [<ExceptionDiagnoser>]
    [<Config(typeof<AntiVirusFriendlyConfig>)>]
    type Runner() =
        let random = randomBuilder consoleOutputHelper
        
        let numberSetData =
            [0..100]
            |> Seq.map decimal
            |> Array.ofSeq
            
        let stringSetData =
            numberSetData
            |> Seq.map (
                toString
                >> flip (+) "strdata_")
            |> Array.ofSeq
            
        let binarySetData =
            stringSetData
            |> Seq.map Encoding.UTF8.GetBytes
            |> Array.ofSeq
            
        let setBuilder setData build () =
            [0..random.Next(10)]
            |> Seq.map (fun _ -> random.Next(stringSetData.Length))
            |> Seq.distinct
            |> Seq.map (Array.get setData >> build)
            |> AttributeSet.create
            
        let stringSetBuilder = setBuilder stringSetData AttributeValue.createString
        let numberSetBuilder = setBuilder numberSetData AttributeValue.createNumber
        let binarySetBuilder = setBuilder binarySetData AttributeValue.createBinary
            
        let averageNumAttributes = 20
            
        let rec randomVal depth =
            match random.Next(10) with
            | 0 -> AttributeValue.createNull ()
            | 1 -> AttributeValue.createString (Array.get stringSetData (random.Next(stringSetData.Length)))
            | 2 -> AttributeValue.createNumber (Array.get numberSetData (random.Next(numberSetData.Length)))
            | 3 -> AttributeValue.createBinary (Array.get binarySetData (random.Next(binarySetData.Length)))
            | 4 -> AttributeValue.createBoolean (random.Next(2) = 0)
            | 5 -> AttributeValue.createHashMap (buildObj' ValueNone depth 0 (depth * 2 - random.Next averageNumAttributes))
            | 6 -> AttributeValue.createAttributeList (AttributeListType.SparseList Map.empty)
            | 7 -> AttributeValue.createHashSet (stringSetBuilder())
            | 8 -> AttributeValue.createHashSet (numberSetBuilder())
            | 9 -> AttributeValue.createHashSet (binarySetBuilder())
            | x -> invalidOp $"{x}"
        and buildObj' id depth min max =
            if max <= 0 then Map.empty
            elif max < min then invalidOp ""
            else 
                let obj = id ?|> (AttributeValue.createNumber >> flip (Map.add "Pk") Map.empty) ?|? Map.empty
                
                [1..min + random.Next(max - min)]
                |> Seq.map (fun x -> struct ($"p{x}", randomVal (depth + 1)))
                |> Seq.fold (fun m struct (k, v) -> Map.add k v m) obj
                
        let buildObj totalObjs =
            buildObj' (random.Next(totalObjs / 2) |> decimal |> ValueSome) 0 2 20
        
        let build100kWrites () =
            let total = 100_000
            [1..total]
            |> Seq.map (fun _ -> buildObj total)
            |> Seq.map itemToDynamoDb
            |> Array.ofSeq
        
        let writeItems (items: Dictionary<string, DynamoAttributeValue> seq) =
            
            task {
                use db = new TestDynamo.Api.FSharp.Database()
                use csdb = new TestDynamo.Api.Database(db)
                csdb
                    .TableBuilder("TestTable", ("Pk", "N"))
                    .AddTable()
                
                use client = TestDynamoClient.createClient<AmazonDynamoDBClient> ValueNone ValueNone (ValueSome db)
                TestDynamoClient.setProcessingDelay TimeSpan.Zero client
                for i in items do
                    do! client.PutItemAsync("TestTable", i) |> Io.ignoreTask
                
                ()
            }
           
        let mutable testItems = [||]   
            
        [<GlobalSetup>]
        member _.Setup() =
            testItems <- build100kWrites()
        
        [<Benchmark>]
        member this.Run () =

            writeItems testItems
            // use runner = AssemblyRunner.WithoutAppDomain """C:\Dev\TestDynamo\TestDynamo.Tests\bin\Debug\net8.0\TestDynamo.Tests.dll"""
            // runner.Start()
            //
            // // runner.OnTestPassed <- fun x ->
            // //     Console.WriteLine("Passed " + x.TestDisplayName)
            // //     ()
            // // runner.OnTestFailed <- fun x ->
            // //     Console.WriteLine("Failed " + x.TestDisplayName)
            // //     ()
            //
            // Thread.Sleep(2000)        
            // while runner.Status <> AssemblyRunnerStatus.Idle do
            //     Thread.Sleep(1000)      
    
    let [<EntryPoint>] main _ =
        Console.WriteLine("Starting")
        
        let summary = BenchmarkRunner.Run<Runner>()
        0
        // let start = DateTime.UtcNow
        // let items = build100kWrites ()
        // Console.WriteLine($"Built items 1 {DateTime.UtcNow - start}")
        //
        // let start = DateTime.UtcNow
        // (writeItems items).ConfigureAwait(false).GetAwaiter().GetResult()
        // Console.WriteLine($"Pushed items 1 {DateTime.UtcNow - start}")
        //
        // // let start = DateTime.UtcNow
        // // (writeItems items).ConfigureAwait(false).GetAwaiter().GetResult()
        // // Console.WriteLine($"Pushed items 2 {DateTime.UtcNow - start}")
        // //
        // // let start = DateTime.UtcNow
        // // (writeItems items).ConfigureAwait(false).GetAwaiter().GetResult()
        // // Console.WriteLine($"Pushed items 3 {DateTime.UtcNow - start}")
        //
        // Console.WriteLine($"Done")
        //
        // 0
        

        // let t = typedefof<AvlTreeTests>
        //
        // let mem =
        //     t.GetMembers(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
        //     |> Array.filter (fun m ->
        //         m.GetCustomAttributes()
        //         |> Seq.filter (function
        //             | :? FactAttribute as _ -> true
        //             | _ -> false)
        //         |> Seq.isEmpty
        //         |> not)
        //     |> Array.map (fun m -> $"{m.Name} {m.MemberType}")
        //     |> Str.join "\n"
        //
        // // Console.WriteLine(mem)

        // let config = new ManualConfig()

        // let config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator)
        //     // .WithOptions(ConfigOptions.DisableOptimizationsValidator)
        //     // .AddValidator(JitOptimizationsValidator.DontFailOnError)
        //     // .AddLogger(ConsoleLogger.Default)
        //     // .AddColumnProvider(DefaultColumnProviders.Instance);  
        //
        // let summary = BenchmarkRunner.Run<Runner>(config);
        //
        // 0
