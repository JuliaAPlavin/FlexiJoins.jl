using FlexiJoins
using FlexiJoins: normalize_arg, ByKey, Mode
using StructArrays, TypedTables
using StaticArrays
using Dictionaries: dictionary
using IntervalSets
using Distances
using Test


function test_unique_setequal(a, b)
    @test allunique(a)
    @test allunique(b)
    @test issetequal(a, b)
end


objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]

@testset "basic" begin
    @test flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj))) ==
        [(O=(obj="A", value=2), M=(obj="A", time=8)), (O=(obj="A", value=2), M=(obj="A", time=12)), (O=(obj="A", value=2), M=(obj="A", time=16)), (O=(obj="A", value=2), M=(obj="A", time=20)), (O=(obj="B", value=-5), M=(obj="B", time=2))]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj))) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5)]
    @test joinindices((;O=objects, M=[(name=x.obj,) for x in measurements]), by_key((O=@optic(_.obj), M=@optic(_.name)))) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5)]

    J = flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj)))
    JI = joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)))
    @test parentindices(J.O) == JI.O
    @test parentindices(J.M) == JI.M

    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=(O=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing)]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=(M=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=nothing, M=6), (O=nothing, M=7), (O=nothing, M=8)]
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=keep),
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing), (O=nothing, M=6), (O=nothing, M=7), (O=nothing, M=8)]
    )

    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,)) ==
        [(O=1, M=1), (O=2, M=5)]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=last,)) ==
        [(O=1, M=4), (O=2, M=5)]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,), nonmatches=(O=keep,)) ==
        [(O=1, M=1), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing)]

    @test flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O) ==
        [(O=(obj="A", value=2), M=[(obj="A", time=8), (obj="A", time=12), (obj="A", time=16), (obj="A", time=20)]), (O=(obj="B", value=-5), M=[(obj="B", time=2)])]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O) ==
        [(O=1, M=[1, 2, 3, 4]), (O=2, M=[5])]
    @test_broken joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:M)
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=1, M=[1, 2, 3, 4]), (O=2, M=[5]), (O=3, M=[]), (O=4, M=[]), (O=nothing, M=[6, 7, 8])]
    )
    test_unique_setequal(
        flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=(obj="A", value=2), M=[(obj="A", time=8), (obj="A", time=12), (obj="A", time=16), (obj="A", time=20)]), (O=(obj="B", value=-5), M=[(obj="B", time=2)]), (O=(obj="D", value=1), M=[]), (O=(obj="E", value=9), M=[]), (O=nothing, M=[(obj="C", time=6), (obj="C", time=9), (obj="C", time=12)])]
    )
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3))),
        joinindices((;O=objects, M=measurements), by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3))),
        joinindices((;O=objects, M=measurements), by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices((;M=measurements, O=objects), by_distance(:time, :value, Euclidean(), <=(3))),
        joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value-3)..(x.value+3))),
    )
    @test_broken isempty(joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value+3)..(x.value-3))))
    test_unique_setequal(
        rightjoin((;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3))),
        rightjoin((;O=objects, M=measurements), by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        rightjoin((objects, measurements), by_distance(:value, :time, Euclidean(), <=(3))),
        rightjoin((objects, measurements), by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    @test joinindices((;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,)) ==
        [(O=1, M=5), (O=3, M=5), (O=4, M=7)]
    @test joinindices((;O=objects, M=measurements), by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=6), (O=2, M=5), (O=3, M=5), (O=4, M=2)]
    @test joinindices((;O=objects, M=measurements), by_pred(:value, >, :time); multi=(M=closest,)) ==
        [(O = 4, M = 1)]
    @test joinindices((;O=objects, M=measurements), by_key(:obj) & by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=1), (O=2, M=5)]

    @test_throws ErrorException joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,), nonmatches=keep)
end

@testset "cardinality" begin
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=1, M=1))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=*, M=0))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=0, M=*))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=*, M=+))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=+, M=*))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=+, M=+))
    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(M=+,))
    joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(M=*,))
end

function test_modes(modes, args...; kwargs...)
    base = joinindices(args...; kwargs..., mode=Mode.NestedLoop())
    @testset for mode in [nothing; modes]
        cur = joinindices(args...; kwargs..., mode)
        test_unique_setequal(cur, base)
    end
end

@testset "join modes" begin
    @testset for (cond, modes) in [
            (by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()]),
            (by_distance(:value, :time, Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()]),
            (by_pred(:obj, ==, :obj), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, <, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, <=, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, >, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, >=, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> x.time..(x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:open,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:closed,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:open,:closed}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_key(@optic(_.obj)) & by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort()]),
        ]
        test_modes(modes, (;O=objects, M=measurements), cond)
        test_modes(modes, (;O=objects, M=measurements), cond; nonmatches=(O=keep,))
        test_modes(modes, (;O=objects, M=measurements), cond; nonmatches=keep)

        base = joinindices((;O=objects, M=measurements), cond)
        cache = join_cache()
        @test isnothing(cache.prepared)
        test_unique_setequal(joinindices((;O=objects, M=measurements), cond; cache), base)
        @test !isnothing(cache.prepared)
        test_unique_setequal(joinindices((;O=objects, M=measurements), cond; cache), base)
        @test !isnothing(cache.prepared)
        @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:abc); cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), cond; multi=(M=first,), cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), cond; mode=Mode.NestedLoop(), cache)

        # fails when join sides need to be swapped, i.e. by_pred(∈):
        # test_modes(modes, (;O=objects, M=measurements), cond; multi=(M=first,))
        # order within groups may differ, so tests fail:
        # test_modes(modes, (;O=objects, M=measurements), cond; groupby=:O)
        # test_modes(modes, (;O=objects, M=measurements), cond; groupby=:O, nonmatches=keep)
    end
    test_modes([Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,))
    test_modes([Mode.NestedLoop(), Mode.Sort()], (;O=objects, M=measurements), by_pred(:value, <, :time); multi=(M=closest,))
end

@testset "normalize_arg" begin
    @test normalize_arg(by_key(@optic(_.obj)), (A=[], B=[])) == ByKey(((@optic(_.obj),), (@optic(_.obj),)))
    @test normalize_arg(by_key(:obj), (A=[], B=[])) == ByKey(((@optic(_.obj),), (@optic(_.obj),)))
    @test normalize_arg(by_key(:obj), ([], [])) == ByKey(((@optic(_.obj),), (@optic(_.obj),)))
    @test normalize_arg(by_key((:obj, @optic(_.name))), ([], [])) == ByKey(((@optic(_.obj), @optic(_.name)), (@optic(_.obj), @optic(_.name))))
    @test normalize_arg(by_key((A=@optic(_.name), B=:obj)), (A=[], B=[])) == ByKey(((@optic(_.name),), (@optic(_.obj),)))
end

@testset "other types" begin
    @testset "tuple" begin
        @test flexijoin((objects, measurements), by_key(@optic(_.obj))) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "structarray" begin
        @test flexijoin((objects |> StructArray, measurements |> StructArray), by_key(@optic(_.obj))) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "typedtable" begin
        @test flexijoin((objects |> Table, measurements |> Table), by_key(@optic(_.obj))) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "dictionary" begin
        @test_broken flexijoin((objects, dictionary('a':'h' .=> measurements)), by_key(@optic(_.obj))) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end
end

@testset "show" begin
    @test string(by_key((:a, :b)) & by_key(:key) & by_pred(:id, <, :id1) & by_distance(:time, Euclidean(), <=(3))) ==
        "by_key((:a, :b)) & by_key(key) & by_pred(id < id1) & by_distance(Euclidean(0.0)(time, time) <= 3.0)"
end


import CompatHelperLocal as CHL
CHL.@check()

import Aqua
Aqua.test_all(FlexiJoins; ambiguities=false)
