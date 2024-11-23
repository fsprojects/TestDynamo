
namespace TestDynamo.Tests

open System.Runtime.InteropServices
open Amazon
open Amazon.DynamoDBv2
open Amazon.Runtime
open TestDynamo
open TestDynamo.Utils
open Tests.Utils
open Xunit
open Xunit.Abstractions

type Ok1NoCreds(
    [<Optional; DefaultParameterValue(null: RegionEndpoint)>] region: RegionEndpoint,
    [<Optional; DefaultParameterValue(7)>] anInt: int) =
    inherit AmazonDynamoDBClient(region)

    member _.AnInt = anInt

type Ok2NoCreds(region: RegionEndpoint) =
    inherit AmazonDynamoDBClient(region)

type Ok1WithCreds(
    [<Optional; DefaultParameterValue(null: RegionEndpoint)>] region: RegionEndpoint,
    [<Optional; DefaultParameterValue(null: AWSCredentials)>] creds: AWSCredentials,
    [<Optional; DefaultParameterValue(7)>] anInt: int) =
    inherit AmazonDynamoDBClient(creds, region)

    member _.AnInt = anInt

type Ok1WithMandatoryCreds(
    creds: AWSCredentials,
    [<Optional; DefaultParameterValue(null: RegionEndpoint)>] region: RegionEndpoint,
    [<Optional; DefaultParameterValue(7)>] anInt: int) =
    inherit AmazonDynamoDBClient(creds, region)

    member _.AnInt = anInt

type Ok2WithCreds(region: RegionEndpoint, creds: AWSCredentials) =
    inherit AmazonDynamoDBClient(creds, region)

type Fail1() =
    inherit AmazonDynamoDBClient()

type Fail2(
    region: RegionEndpoint,
    anInt: int) =
    inherit AmazonDynamoDBClient(region)

    member _.AnInt = anInt

type CreateClientTests(output: ITestOutputHelper) =

    [<Fact>]
    let ``CreateClient, smoke tests`` () =

        task {
            // arrange
            // act
            let errors =
                [
                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Fail1>(addAwsCredentials = false) |> ignore)
                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Fail1>(addAwsCredentials = true) |> ignore)
                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Fail2>(addAwsCredentials = false) |> ignore)
                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Fail2>(addAwsCredentials = true) |> ignore)

                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Ok1NoCreds>(addAwsCredentials = true) |> ignore)
                    Assert.ThrowsAny(fun () -> TestDynamoClient.CreateClient<Ok1WithMandatoryCreds>(addAwsCredentials = false) |> ignore)
                ]

            use client1 = TestDynamoClient.CreateClient<Ok1NoCreds>(addAwsCredentials = false)
            use client2 = TestDynamoClient.CreateClient<Ok1WithMandatoryCreds>(addAwsCredentials = true)
            use client22 = TestDynamoClient.CreateClient<Ok1WithCreds>(addAwsCredentials = true)
            use _ = TestDynamoClient.CreateClient<Ok2NoCreds>(addAwsCredentials = false)
            use _ = TestDynamoClient.CreateClient<Ok2WithCreds>(addAwsCredentials = true)

            // assert
            Assert.Equal(7, client1.AnInt)
            Assert.Equal(7, client2.AnInt)
            Assert.Equal(7, client22.AnInt)

            List.fold (assertError output $"{nameof TestDynamoClient}.Attach" |> asLazy) () errors
        }