open System
open System.Threading
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Running
open Xunit.Runners

module Program =

    [<MemoryDiagnoser>]
    // [<InliningDiagnoser>]
    // [<TailCallDiagnoser>]
    // [<EtwProfiler>]
    // [<ConcurrencyVisualizerProfiler>]
    // [<NativeMemoryProfiler>]
    // [<ThreadingDiagnoser>]
    [<ExceptionDiagnoser>]
    type Runner() =
        [<Benchmark>]
        member this.Run () =

            use runner = AssemblyRunner.WithoutAppDomain """C:\Dev\TestDynamo\TestDynamo.Tests\bin\Debug\net8.0\TestDynamo.Tests.dll"""
            runner.Start()

            // runner.OnTestPassed <- fun x ->
            //     Console.WriteLine("Passed " + x.TestDisplayName)
            //     ()
            // runner.OnTestFailed <- fun x ->
            //     Console.WriteLine("Failed " + x.TestDisplayName)
            //     ()

            Thread.Sleep(2000)        
            while runner.Status <> AssemblyRunnerStatus.Idle do
                Thread.Sleep(1000)      

    let [<EntryPoint>] main _ =
        Console.WriteLine("Starting")

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

        let config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator)
            // .WithOptions(ConfigOptions.DisableOptimizationsValidator)
            // .AddValidator(JitOptimizationsValidator.DontFailOnError)
            // .AddLogger(ConsoleLogger.Default)
            // .AddColumnProvider(DefaultColumnProviders.Instance);  

        let summary = BenchmarkRunner.Run<Runner>(config);

        0
