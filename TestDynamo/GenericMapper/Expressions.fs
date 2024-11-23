module TestDynamo.GenericMapper.Expressions

open System
open System.Reflection
open System.Threading
open Microsoft.FSharp.Core
open TestDynamo.Data.Monads.Operators
open System.Linq.Expressions
open TestDynamo.Utils
open TestDynamo.GenericMapper.Utils

type Expr (expr: Expression) =

    let mutable count =
        match expr with
        | :? ParameterExpression
        | :? ConstantExpression -> Int32.MinValue
        | _ -> 0

    member _.Type = expr.Type

    member _.Expr
        with get() =
            let c = Interlocked.Increment(&count)
            assert (c <= 1)
            expr

    override _.ToString() = expr.ToString()

[<RequireQualifiedAccess>]
module Expr =
    let private expr' x = Expr(x)
    let private getExpr (x: Expr) = x.Expr

    let callStatic method args = Expression.Call(method, args |> Seq.map getExpr) |> expr'
    let call method instance args = Expression.Call(getExpr instance, method, args |> Seq.map getExpr) |> expr'
    let propStatic (prop: MemberInfo) = Expression.MakeMemberAccess(null, prop) |> expr'
    let prop name instance = Expression.PropertyOrField(instance |> getExpr, name) |> expr'
    let variable t = Expression.Variable t
    let param t = Expression.Parameter t
    let block (t: Type voption) block =
        t
        ?|> fun t -> Expression.Block(t, block |> Seq.map getExpr)
        ?|>? fun _ -> Expression.Block(block |> Seq.map getExpr)
        |> expr'
    let constant x = Expression.Constant x |> expr'
    let constantT t x = Expression.Constant(x, t) |> expr'
    let constantNull t = Expression.Constant(null, t) |> expr'
    let assign l r = Expression.Assign(getExpr l, getExpr r) |> expr'
    let equal l r = Expression.Equal(getExpr l, getExpr r) |> expr'
    let newObj constructor (args: _ seq) = Expression.New(constructor, args |> Seq.map getExpr) |> expr'
    let convert t x = Expression.Convert(getExpr x, t) |> expr'
    let condition ``if`` ``then`` ``else`` =
        Expression.Condition(getExpr ``if``, getExpr ``then``, getExpr ``else``) |> expr'

    let ifThen ``if`` ``then`` = Expression.IfThen(getExpr ``if``, getExpr ``then``) |> expr'

    let private tryLambda' paramTypes body =
        let ps = paramTypes |> Seq.map Expression.Parameter |> List.ofSeq
        ps |> List.map expr' |> body
        ?|> fun b -> Expression.Lambda(getExpr b, ps)

    let private lambda' paramTypes body =
        tryLambda' paramTypes (body >> ValueSome) |> Maybe.expectSome

    let lambda0: Expr -> LambdaExpression = asLazy >> lambda' []
    let lambda1 paramType body =
        lambda' [paramType] (function | [x] -> body x | _ -> invalidOp "An unexpected error has occurred")
    let tryLambda1 paramType body =
        tryLambda' [paramType] (function | [x] -> body x | _ -> invalidOp "An unexpected error has occurred")
    let lambda2 paramType1 paramType2 body =
        lambda' [paramType1; paramType2] (function | [x1; x2] -> body x1 x2 | _ -> invalidOp "An unexpected error has occurred")
    let lambda3 paramType1 paramType2 paramType3 body =
        lambda' [paramType1; paramType2; paramType3] (function | [x1; x2; x3] -> body x1 x2 x3 | _ -> invalidOp "An unexpected error has occurred")

    let compile (lambda: LambdaExpression) = lambda.Compile()

    let cache f (expr: Expr) =
        let param = Expression.Parameter expr.Type
        let result = f (expr' param)
        let body =
            [
                Expression.Assign(param, expr.Expr) :> Expression
                result |> getExpr
            ]

        Expression.Block(result.Type, [param], body) |> expr'

    let maybeCache f (expr: Expr) =
        let param = Expression.Parameter expr.Type

        expr' param
        |> f
        ?|> (getExpr >> fun result ->
            let body =
                [
                    Expression.Assign(param, expr.Expr) :> Expression
                    result
                ]

            Expression.Block(result.Type, [param], body) |> expr')

    let maybeMutate mutations (expr: Expr) =

        let param = Expression.Parameter expr.Type
        mutations
        |> Seq.map (apply (expr' param))
        |> allOrNone
        ?|> (
            Seq.map getExpr
            >> Collection.prepend (Expression.Assign(param, expr.Expr))
            >> fun body ->
                Expression.Block(expr.Type, [param], body |> Collection.append param) |> expr')

    let mutate: (Expr -> Expr) seq -> Expr -> Expr =
        Seq.map (flip (>>) ValueSome)
        >> maybeMutate
        >>> Maybe.expectSome
