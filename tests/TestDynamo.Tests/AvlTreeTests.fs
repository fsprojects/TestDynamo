
namespace TestDynamo.Tests

open System
open Tests.Utils
open Xunit
open Xunit.Abstractions
open TestDynamo.Data
open TestDynamo.Utils

type AvlTreeTests(output: ITestOutputHelper) =

    let aBunchOfValues (rand: Random) diff =

        [0..diff..100 * diff]
        |> Seq.map (fun i -> struct (rand.Next(), struct (i, System.Guid.NewGuid())))
        |> Seq.sortBy fstT
        |> Seq.map sndT
        |> Array.ofSeq

    let avlTreeWithABunchOfValues (rand: Random) diff =

        let inserts = aBunchOfValues rand diff

        let result = Array.fold (fun s struct (k, v) -> AvlTree.addOrReplace k v s |> sndT) (AvlTree.empty LanguagePrimitives.FastGenericComparer<int>) inserts

        Array.fold (fun _ struct (k, v) ->
            Assert.Equal(v, AvlTree.find k result)
            ) () inserts

        struct (inserts, result)

    [<Theory>]
    [<InlineData(1, true, true)>]
    [<InlineData(1, true, false)>]
    [<InlineData(1, false, true)>]
    [<InlineData(1, false, false)>]
    [<InlineData(2, true, true)>]
    [<InlineData(2, true, false)>]
    [<InlineData(2, false, true)>]
    [<InlineData(2, false, false)>]
    let ``101 items inserted randomly, get single item spread`` diff inclusive forwards =
        // arrange
        let rand = randomBuilder output
        //let rand = seededRandomBuilder 1950387206 output
        let targetIndex = rand.Next(100)
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff
        let struct (targetK, targetV) = Array.get vals targetIndex

        // act
        let result = AvlTree.seek (ValueSome targetK) (ValueSome targetK) inclusive forwards tree

        // assert
        if inclusive
        then Assert.Equal([struct (targetK, targetV)], result)
        else Assert.Empty(result)

    [<Theory>]
    [<InlineData(1, true, true)>]
    [<InlineData(1, true, false)>]
    [<InlineData(1, false, true)>]
    [<InlineData(1, false, false)>]
    [<InlineData(2, true, true)>]
    [<InlineData(2, true, false)>]
    [<InlineData(2, false, true)>]
    [<InlineData(2, false, false)>]
    let ``101 items inserted randomly, get range from x to y``  diff inclusive forwards =

        // arrange
        let rand = randomBuilder output
        //let rand = seededRandomBuilder 243402555 output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff

        let targetStart = Array.get vals (rand.Next(Array.length vals - 1)) |> fstT
        let targetEnd = Array.get vals (rand.Next(Array.length vals - 1)) |> fstT
        let struct (targetStart, targetEnd) =
            match struct (forwards, targetStart < targetEnd) with
            | false, true
            | true, false -> struct (targetEnd, targetStart)
            | false, false
            | true, true -> struct (targetStart, targetEnd)

        let expected =
            let d = if forwards then diff else -diff

            if inclusive then [targetStart..d..targetEnd]
            elif targetStart = targetEnd then []
            else [targetStart + d..d..targetEnd - d]

        // act
        let result = AvlTree.seek (ValueSome targetStart) (ValueSome targetEnd) inclusive forwards tree

        // assert
        output.WriteLine($"From {targetStart} to {targetEnd}, forwards {forwards}")
        Assert.Equal(expected, result |> Seq.map fstT)

    [<Theory>]
    [<InlineData(1, true)>]
    [<InlineData(1, false)>]
    [<InlineData(2, true)>]
    [<InlineData(2, false)>]
    let ``101 items inserted randomly, get range from x, forwards``  diff inclusive =
        // arrange
        let rand = randomBuilder output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff
        let vals = vals |> Array.sortBy fstT

        let targetStartI = rand.Next(Array.length vals - 1)
        let resultStartI =
            match inclusive with
            | true -> targetStartI
            | false -> targetStartI + 1
        let targetStart = Array.get vals targetStartI |> fstT

        let expected = vals |> Seq.sortBy fstT |> Seq.skip resultStartI

        // act
        let result = AvlTree.seek (ValueSome targetStart) ValueNone inclusive true tree

        // assert
        Assert.Equal<struct (int * Guid)>(expected, result)

    [<Theory>]
    [<InlineData(1, true)>]
    [<InlineData(1, false)>]
    [<InlineData(2, true)>]
    [<InlineData(2, false)>]
    let ``101 items inserted randomly, get range from x, backwards``  diff inclusive =
        // arrange
        let rand = randomBuilder output
        //let rand = seededRandomBuilder 1968229267 output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff
        let vals = vals |> Array.sortBy fstT

        let targetStartI = rand.Next(Array.length vals - 1)
        let targetStart = Array.get vals targetStartI |> fstT

        let resultStartI =
            match inclusive with
            | true -> targetStartI + 1
            | false -> targetStartI
        let expected = vals |> Seq.take resultStartI |> Seq.rev

        // act
        let result = AvlTree.seek (ValueSome targetStart) ValueNone inclusive false tree

        // assert
        output.WriteLine($"Start: {targetStart} {resultStartI}")
        Assert.Equal<struct (int * Guid)>(expected, result)

    [<Theory>]
    [<InlineData(1, true)>]
    [<InlineData(1, false)>]
    [<InlineData(2, true)>]
    [<InlineData(2, false)>]
    let ``101 items inserted randomly, get range to y, forwards``  diff inclusive =
        // arrange
        let rand = randomBuilder output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff
        let vals = vals |> Array.sortBy fstT

        let targetStartI = rand.Next(Array.length vals - 1)
        let resultStartI = if inclusive then targetStartI + 1 else targetStartI
        let targetStart = Array.get vals targetStartI |> fstT

        let expected = vals |> Seq.sortBy fstT |> Seq.take resultStartI

        // act
        let result = AvlTree.seek ValueNone (ValueSome targetStart) inclusive true tree

        // assert
        Assert.Equal<struct (int * Guid)>(expected, result)

    [<Theory>]
    [<InlineData(1, true)>]
    [<InlineData(1, false)>]
    [<InlineData(2, true)>]
    [<InlineData(2, false)>]
    let ``101 items inserted randomly, get range to y, backwards``  diff inclusive =
        // arrange
        let rand = randomBuilder output
        //let rand = seededRandomBuilder 1968229267 output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff
        let vals = vals |> Array.sortBy fstT

        let targetStartI = rand.Next(Array.length vals - 1)
        let targetStart = Array.get vals targetStartI |> fstT

        let resultStartI =
            match inclusive with
            | true -> targetStartI
            | false -> targetStartI + 1
        let expected = vals |> Seq.skip resultStartI |> Seq.rev

        // act
        let result = AvlTree.seek ValueNone (ValueSome targetStart) inclusive false tree

        // assert
        output.WriteLine($"Start: {targetStart} {resultStartI}")
        Assert.Equal<struct (int * Guid)>(expected, result)

    [<Theory>]
    [<InlineData(1, true, true)>]
    [<InlineData(1, true, false)>]
    [<InlineData(1, false, false)>]
    [<InlineData(1, false, true)>]
    [<InlineData(2, true, false)>]
    [<InlineData(2, true, true)>]
    [<InlineData(2, false, false)>]
    [<InlineData(2, false, true)>]
    let ``101 items inserted randomly, get range from beginning to end, includes everything``  diff inclusive forwards =
        // arrange
        let rand = randomBuilder output
        let struct (vals, tree) = avlTreeWithABunchOfValues rand diff

        let expected = Array.sortBy fstT vals |> Array.map fstT

        let struct (first, last) =
            if inclusive then struct (Array.head expected, Array.last expected)
            else struct (Array.head expected - 1, Array.last expected + 1)

        let struct (first, last) =
            if forwards then struct (first, last)
            else struct (last, first)

        // act
        let result = AvlTree.seek (ValueSome first) (ValueSome last) inclusive forwards tree

        // assert
        let result = if forwards then result else Seq.rev result
        Assert.Equal(expected, result |> Seq.map fstT)
