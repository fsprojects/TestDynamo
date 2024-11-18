```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.22631.4460/23H2/2023Update/SunValley3)
AMD Ryzen 7 5800H with Radeon Graphics, 1 CPU, 16 logical and 8 physical cores
.NET SDK 8.0.307
  [Host]     : .NET 8.0.11 (8.0.1124.51707), X64 RyuJIT AVX2 DEBUG
  DefaultJob : .NET 8.0.11 (8.0.1124.51707), X64 RyuJIT AVX2


```
| Method | Mean    | Error   | StdDev  | Gen0        | Exceptions | Gen1       | Gen2      | Allocated |
|------- |--------:|--------:|--------:|------------:|-----------:|-----------:|----------:|----------:|
| Run    | 14.24 s | 0.050 s | 0.047 s | 713000.0000 |          - | 47000.0000 | 4000.0000 |   5.53 GB |
