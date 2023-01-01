using FlexiJoins
using FlexiJoins: normalize_arg, ByKey, Mode
using StructArrays, TypedTables
using StaticArrays
using OffsetArrays
using Dictionaries: dictionary
using IntervalSets
using Distances
using DataPipes
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
    @test joinindices((;O=objects, M=[(name=x.obj,) for x in measurements]), by_key(O=@optic(_.obj), M=@optic(_.name))) ==
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
    @test joinindices((objects, measurements), by_key(@optic(_.obj)); groupby=1) ==
        [(1, [1, 2, 3, 4]), (2, [5])]
    @test_broken joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:M)
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=1, M=[1, 2, 3, 4]), (O=2, M=[5]), (O=3, M=[]), (O=4, M=[]), (O=nothing, M=[6, 7, 8])]
    )
    test_unique_setequal(
        flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=(obj="A", value=2), M=[(obj="A", time=8), (obj="A", time=12), (obj="A", time=16), (obj="A", time=20)]), (O=(obj="B", value=-5), M=[(obj="B", time=2)]), (O=(obj="D", value=1), M=[]), (O=(obj="E", value=9), M=[]), (O=nothing, M=[(obj="C", time=6), (obj="C", time=9), (obj="C", time=12)])]
    )
    @test_broken isempty(joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value+3)..(x.value-3))))
    @test joinindices((;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,)) ==
        [(O=1, M=5), (O=3, M=5), (O=4, M=7)]
    @test joinindices((;O=objects, M=measurements), by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=6), (O=2, M=5), (O=3, M=5), (O=4, M=2)]
    @test joinindices((;O=objects, M=measurements), by_pred(:value, >, :time); multi=(M=closest,)) ==
        [(O = 4, M = 1)]
    @test joinindices((;O=objects, M=measurements), by_key(:obj) & by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=1), (O=2, M=5)]

    @test_throws ErrorException joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,), nonmatches=keep)
    @test_throws ErrorException joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,), groupby=:M)
end

@testset "not_same" begin
    @test_throws Exception joinindices((M1=copy(measurements), M2=measurements), by_key(:obj) & not_same())
    LR = (M1=measurements, M2=measurements)
    @test joinindices(LR, not_same()) ==
        @p joinindices(LR, by_key(Returns(nothing))) |> filter(_.M1 != _.M2)
    @test joinindices(LR, not_same(order_matters=false)) ==
        @p joinindices(LR, by_key(Returns(nothing))) |> filter(_.M1 < _.M2)
    @test joinindices(LR, by_key(:obj) & not_same()) ==
        @p joinindices(LR, by_key(:obj)) |> filter(_.M1 != _.M2)
    @test joinindices(LR, by_key(:obj) & not_same(order_matters=false)) ==
        @p joinindices(LR, by_key(:obj)) |> filter(_.M1 < _.M2)
end

@testset "consistent" begin
    LR = (O=objects, M=measurements)
    
    @test joinindices(LR, by_key((:obj,))) == joinindices(LR, by_key(:obj))
    @test joinindices(LR, by_key((:obj, :obj))) == joinindices(LR, by_key(:obj))
    @test joinindices(LR, by_key((:obj, :obj), x -> (x.obj, x.obj))) == joinindices(LR, by_key(:obj))

    @test joinindices(LR, by_pred(:obj, ==, :obj)) == joinindices(LR, by_key(:obj))
    @test joinindices(LR, by_pred(x -> x.obj == "B" ? nothing : x.obj, ==, :obj)) == joinindices(LR, by_key(x -> x.obj == "B" ? nothing : x.obj, :obj))
    @test joinindices(LR, by_pred(:obj, ∈, x -> (x.obj,))) == joinindices(LR, by_key(:obj))
    @test joinindices(LR, by_pred(:obj, ∈, x -> (nothing, x.obj))) == joinindices(LR, by_key(:obj))
    @test joinindices(LR, by_pred(:obj, ∈, x -> (x.obj, nothing))) == joinindices(LR, by_key(:obj))

    test_unique_setequal(
        joinindices(LR, by_distance(:value, :time, Euclidean(), <=(3))),
        joinindices(LR, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices(LR, by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3))),
        joinindices(LR, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices((;M=measurements, O=objects), by_distance(:time, :value, Euclidean(), <=(3))),
        joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value-3)..(x.value+3))),
    )
    test_unique_setequal(
        joinindices(LR, by_distance(:value, :time, Euclidean(), <=(1))),
        joinindices(LR, by_pred(x -> (x.value, x.value+1, x.value-1), ∋, :time)),
    )
    test_unique_setequal(
        rightjoin(LR, by_distance(:value, :time, Euclidean(), <=(3))),
        rightjoin(LR, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
end

@testset "unnested" begin
    let
        J1 = innerjoin((O=objects, M1=measurements), by_key(:obj))

        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test map(:O, J2.J) == J3.O
        @test map(:M1, J2.J) == J3.M1
        @test J2.M2 == J3.M2

        J4 = innerjoin((_=J1, __=StructArray(measurements)), by_key(:obj ∘ :O, :obj))
        @test map(:O, J2.J) == J4.O
        @test map(:M1, J2.J) == J4.M1
        @test map(:obj, J2.M2) == J4.obj
        @test map(:time, J2.M2) == J4.time

        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj); groupby=:M2)
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj); groupby=:M2)
        @test map(r -> map(:O, r), J2.J) == J3.O
        @test map(r -> map(:M1, r), J2.J) == J3.M1
        @test J2.M2 == J3.M2
    end

    let
        J1 = innerjoin((O=objects, M1=measurements), by_key(:obj); groupby=:O)
        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test map(:O, J2.J) == J3.O
        @test map(:M1, J2.J) == J3.M1
        @test J2.M2 == J3.M2
    end
end

@testset "types" begin
    @testset "container" begin
        J = flexijoin((;O=objects, M=measurements), by_key(:obj))
        @test J isa StructArray
        @test J.O isa FlexiJoins.SentinelView
        @test J.M isa FlexiJoins.SentinelView
        Jm = FlexiJoins.materialize_views(J)
        @test Jm isa StructArray
        @test Jm.O isa Vector{<:NamedTuple}
        @test Jm.M isa Vector{<:NamedTuple}

        J = flexijoin((;O=objects, M=measurements), by_key(:obj); groupby=:O)
        @test J isa StructArray
        @test J.O isa FlexiJoins.SentinelView
        @test J.M isa Vector{<:FlexiJoins.SentinelView}
        Jm = FlexiJoins.materialize_views(J)
        @test Jm isa StructArray
        @test Jm.O isa Vector{<:NamedTuple}
        @test Jm.M isa Vector{<:Vector}
    end

    @testset "eltype" begin
        J = innerjoin((;O=objects, M=measurements), by_key(:obj))
        @test eltype(J.O) == eltype(objects)
        @test eltype(J.M) == eltype(measurements)
        J = leftjoin((;O=objects, M=measurements), by_key(:obj))
        @test eltype(J.O) == eltype(objects)
        @test eltype(J.M) == Union{Nothing, eltype(measurements)}
        J = rightjoin((;O=objects, M=measurements), by_key(:obj))
        @test eltype(J.O) == Union{Nothing, eltype(objects)}
        @test eltype(J.M) == eltype(measurements)
        J = outerjoin((;O=objects, M=measurements), by_key(:obj))
        @test eltype(J.O) == Union{Nothing, eltype(objects)}
        @test eltype(J.M) == Union{Nothing, eltype(measurements)}
    end
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
    joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=0:4,))
    joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(M=0:1,))
    joinindices((;O=objects, M=measurements), by_key(:obj); cardinality=(O=0:4, M=0:1))
end

function test_modes(modes, args...; alloc=true, kwargs...)
    base = joinindices(args...; kwargs..., mode=Mode.NestedLoop())
    @testset for mode in [nothing; modes]
        cur = joinindices(args...; kwargs..., mode)
        test_unique_setequal(cur, base)

        if alloc && mode != Mode.NestedLoop() && all(!isempty, args[1])
            LR = map(X -> repeat(X, 200), args[1])
            cond = args[2]
            joinindices(LR, Base.tail(args)...; kwargs..., mode)
            timed = @timed joinindices(LR, Base.tail(args)...; kwargs..., mode)
            if cond isa FlexiJoins.ByDistance
                @test_broken Base.gc_alloc_count(timed.gcstats) < 150
            else
                @test Base.gc_alloc_count(timed.gcstats) < 150
            end
        end
    end
end

@testset "join modes" begin
    @testset for (cond, modes) in [
            (by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()]),
            (by_key(x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()]),
            (by_distance(:value, :time, Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()]),
            (by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()]),
            (by_pred(:obj, ==, :obj), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()]),
            (by_pred(:obj, ==, x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()]),
            (by_pred(:value, <, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, <=, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, >, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, >=, :time), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> (x.time, x.time + 5, x.time + 10)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()]),
            (by_pred(:value, ∈, x -> x.time..(x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:open,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:closed,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_pred(:value, ∈, x -> Interval{:open,:closed}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_key(@optic(_.obj)) & by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()]),
            (by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort()]),
        ]
        test_modes(modes, (;O=objects, M=measurements), cond)
        test_modes(modes, (;O=objects[1:0], M=measurements), cond)
        test_modes(modes, (;O=objects, M=measurements[1:0]), cond)
        test_modes(modes, (;O=objects[1:0], M=measurements[1:0]), cond)
        test_modes(modes, (;O=objects, M=measurements), cond; nonmatches=(O=keep,))
        test_modes(modes, (;O=objects, M=measurements), cond; nonmatches=keep)

        first_M = cond isa FlexiJoins.ByPred{typeof(∈)}  # the ∈ condition only supports a single "direction"

        base = joinindices((;O=objects, M=measurements), cond)
        cache = join_cache()
        @test isnothing(cache.prepared)
        test_unique_setequal(joinindices((;O=objects, M=measurements), cond; cache), base)
        @test !isnothing(cache.prepared)
        test_unique_setequal(joinindices((;O=objects, M=measurements), cond; cache), base)
        @test !isnothing(cache.prepared)
        @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(:abc); cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), cond; multi=first_M ? (O=first,) : (M=first,), cache)
        @test_throws AssertionError joinindices((;O=objects, M=measurements), cond; mode=Mode.NestedLoop(), cache)

        test_modes(modes, (;O=objects, M=measurements), cond; multi=first_M ? (O=first,) : (M=first,))
        # order within groups may differ, so tests fail:
        # test_modes(modes, (;O=objects, M=measurements), cond; groupby=:O)
        # test_modes(modes, (;O=objects, M=measurements), cond; groupby=:O, nonmatches=keep)
    end
    test_modes([Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;O=objects, M=measurements), by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,))
    test_modes([Mode.NestedLoop(), Mode.Sort()], (;O=objects, M=measurements), by_pred(:value, <, :time); multi=(M=closest,))
    test_modes([Mode.NestedLoop(), Mode.Hash()], (measurements, measurements), by_key(:obj) & not_same(); alloc=false)
    test_modes([Mode.NestedLoop(), Mode.NestedLoopFast()], (measurements, measurements), not_same(); alloc=false)
end

@testset "normalize_arg" begin
    @test normalize_arg(by_key(@optic(_.obj)), (A=[], B=[])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj), (A=[], B=[])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj), ([], [])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj, @optic(_.name)), ([], [])) == ByKey((@optic(_.obj), @optic(_.name)))
    @test normalize_arg(by_key(A=@optic(_.name), B=:obj), (A=[], B=[])) == ByKey((@optic(_.name), @optic(_.obj)))
end

@testset "other types" begin
    @testset "tuple" begin
        @test flexijoin((objects, measurements), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "structarray" begin
        @test flexijoin((objects |> StructArray, measurements |> StructArray), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "typedtable" begin
        @test flexijoin((objects |> Table, measurements |> Table), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "offsetarray" begin
        @test flexijoin((OffsetArray(objects, -100), measurements), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
        @test flexijoin((objects, OffsetArray(measurements, 1000)), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
        @test flexijoin((OffsetArray(objects, -100), OffsetArray(measurements, 1000)), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end

    @testset "dictionary" begin
        @test flexijoin((objects, dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
        @test flexijoin((dictionary(string.('w':'z') .=> objects), measurements), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
        @test flexijoin((dictionary(string.('w':'z') .=> objects), dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj)) ==
            [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]
    end
end

@testset "show" begin
    @test string(by_key(@optic(_.a[12]), :b) & by_key(:key) & by_pred(:id, <, :id1) & by_distance(:time, Euclidean(), <=(3))) ==
        "by_key((@optic _[12]) ∘ (@optic _.a), (@optic _.b)) & by_key((@optic _.key)) & by_pred((@optic _.id) < (@optic _.id1)) & by_distance(Euclidean(0.0)((@optic _.time), (@optic _.time)) <= 3.0)"
end


import CompatHelperLocal as CHL
CHL.@check()

import Aqua
Aqua.test_all(FlexiJoins; ambiguities=false)
