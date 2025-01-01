using TestItems
using TestItemRunner
@run_package_tests


@testitem "basic" begin
    using Accessors
    using IntervalSets
    using Distances: Euclidean

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
    end

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

    @test flexijoin(OM, by_key(@optic(_.obj))) ==
        [(O=(obj="A", value=2), M=(obj="A", time=8)), (O=(obj="A", value=2), M=(obj="A", time=12)), (O=(obj="A", value=2), M=(obj="A", time=16)), (O=(obj="A", value=2), M=(obj="A", time=20)), (O=(obj="B", value=-5), M=(obj="B", time=2))]
    @test joinindices(OM, by_key(@optic(_.obj))) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5)]
    @test joinindices((;O=objects, M=[(name=x.obj,) for x in measurements]), by_key(O=@optic(_.obj), M=@optic(_.name))) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5)]

    J = flexijoin(OM, by_key(@optic(_.obj)))
    JI = joinindices(OM, by_key(@optic(_.obj)))
    @test parentindices(J.O) == (JI.O,)
    @test parentindices(J.M) == (JI.M,)
    J = flexijoin(OM, by_key(@optic(_.obj)); nonmatches=(M=keep,))
    JI = joinindices(OM, by_key(@optic(_.obj)); nonmatches=(M=keep,))
    @test parentindices(J.O) == (JI.O,)
    @test parentindices(J.M) == (JI.M,)

    @test joinindices(OM, by_key(@optic(_.obj)); nonmatches=(O=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing)]
    @test joinindices(OM, by_key(@optic(_.obj)); nonmatches=(M=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=nothing, M=6), (O=nothing, M=7), (O=nothing, M=8)]
    test_unique_setequal(
        joinindices(OM, by_key(@optic(_.obj)); nonmatches=keep),
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing), (O=nothing, M=6), (O=nothing, M=7), (O=nothing, M=8)]
    )

    @test joinindices(OM, by_key(@optic(_.obj)); multi=(M=first,)) ==
        [(O=1, M=1), (O=2, M=5)]
    @test joinindices(OM, by_key(@optic(_.obj)); multi=(M=last,)) ==
        [(O=1, M=4), (O=2, M=5)]
    @test joinindices(OM, by_key(@optic(_.obj)); multi=(M=first,), nonmatches=(O=keep,)) ==
        [(O=1, M=1), (O=2, M=5), (O=3, M=nothing), (O=4, M=nothing)]

    @test flexijoin(OM, by_key(@optic(_.obj)); groupby=:O) ==
        [(O=(obj="A", value=2), M=[(obj="A", time=8), (obj="A", time=12), (obj="A", time=16), (obj="A", time=20)]), (O=(obj="B", value=-5), M=[(obj="B", time=2)])]
    @test joinindices(OM, by_key(@optic(_.obj)); groupby=:O) ==
        [(O=1, M=[1, 2, 3, 4]), (O=2, M=[5])]
    @test joinindices((objects, measurements), by_key(@optic(_.obj)); groupby=1) ==
        [(1, [1, 2, 3, 4]), (2, [5])]
    @test joinindices(OM, by_key(@optic(_.obj)); groupby=:M) == [(O=[1], M=1), (O=[1], M=2), (O=[1], M=3), (O=[1], M=4), (O=[2], M=5)]
    test_unique_setequal(
        joinindices(OM, by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=1, M=[1, 2, 3, 4]), (O=2, M=[5]), (O=3, M=[]), (O=4, M=[]), (O=nothing, M=[6, 7, 8])]
    )
    test_unique_setequal(
        flexijoin(OM, by_key(@optic(_.obj)); groupby=:O, nonmatches=keep),
        [(O=(obj="A", value=2), M=[(obj="A", time=8), (obj="A", time=12), (obj="A", time=16), (obj="A", time=20)]), (O=(obj="B", value=-5), M=[(obj="B", time=2)]), (O=(obj="D", value=1), M=[]), (O=(obj="E", value=9), M=[]), (O=nothing, M=[(obj="C", time=6), (obj="C", time=9), (obj="C", time=12)])]
    )
    @test isempty(joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value+3)..(x.value-3))))
    @test joinindices((;M=measurements, O=objects), by_pred(:time, ≈, :value; atol=3)) ==
        [(M = 5, O = 1), (M = 5, O = 3), (M = 6, O = 4), (M = 1, O = 4), (M = 7, O = 4), (M = 2, O = 4), (M = 8, O = 4)]
    @test_broken joinindices((;M=measurements, O=objects), by_pred(:time, ≈, :value; atol=3); multi=(M=closest,)) ==
        [(O=1, M=5), (O=3, M=5), (O=4, M=7)]
    @test joinindices(OM, by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,)) ==
        [(O=1, M=5), (O=3, M=5), (O=4, M=7)]
    @test joinindices(OM, by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=6), (O=2, M=5), (O=3, M=5), (O=4, M=2)]
    @test joinindices(OM, by_pred(:value, >, :time); multi=(M=closest,)) ==
        [(O = 4, M = 1)]
    @test joinindices(OM, by_key(:obj) & by_pred(:value, <, :time); multi=(M=closest,)) ==
        [(O=1, M=1), (O=2, M=5)]

    @test_throws "arguments don't make sense together" joinindices(OM, by_key(@optic(_.obj)); multi=(M=first,), nonmatches=keep)
    @test_throws "Unsupported parameter combination" joinindices(OM, by_key(@optic(_.obj)); multi=(M=first,), groupby=:M)
    @test_throws "has no field XXX" joinindices(OM, by_key(@optic(_.obj)); multi=(M=first,), groupby=:XXX)
end

@testitem "not_same" begin
    using DataPipes

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

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

@testitem "consistent" begin
    using IntervalSets
    using Distances: Euclidean
    using StaticArrays: SVector

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
    end

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)
    
    @test joinindices(OM, by_key(:obj)) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_key((:obj, :obj))) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_key((:obj, :obj), x -> (x.obj, x.obj))) == joinindices(OM, by_key(:obj))

    @test joinindices(OM, by_pred(:obj, ==, :obj)) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_pred(:obj, isequal, :obj)) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_pred(x -> x.obj == "B" ? nothing : x.obj, ==, :obj)) == joinindices(OM, by_key(x -> x.obj == "B" ? nothing : x.obj, :obj))
    @test joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,))) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_pred(:obj, ∈, x -> (nothing, x.obj))) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_pred(:obj, ∈, x -> (x.obj, nothing))) == joinindices(OM, by_key(:obj))

    test_unique_setequal(
        joinindices(OM, by_distance(:value, :time, Euclidean(), <=(3))),
        joinindices(OM, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices(OM, by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3))),
        joinindices(OM, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices((;M=measurements, O=objects), by_distance(:time, :value, Euclidean(), <=(3))),
        joinindices((;M=measurements, O=objects), by_pred(:time, ∈, x -> (x.value-3)..(x.value+3))),
    )
    test_unique_setequal(
        joinindices(OM, by_distance(:value, :time, Euclidean(), <=(1))),
        joinindices(OM, by_pred(x -> (x.value, x.value+1, x.value-1), ∋, :time)),
    )
    test_unique_setequal(
        rightjoin(OM, by_distance(:value, :time, Euclidean(), <=(3))),
        rightjoin(OM, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
    )
    test_unique_setequal(
        joinindices(OM, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
        joinindices(OM, by_pred(x -> (x.value-3)..(x.value+3), ⊇, x -> x.time..x.time)),
    )
    test_unique_setequal(
        joinindices(OM, by_pred(x -> (x.value-3)..(x.value+3), ∋, :time)),
        joinindices(OM, by_pred(x -> (x.value-1)..(x.value+2), (!) ∘ isdisjoint, x -> (x.time-1)..(x.time+2))),
    )
end

@testitem "explicit side" begin
    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
    end

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

    @test joinindices(OM, by_key(:obj); loop_over_side=1) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_key(:obj); loop_over_side=2) == joinindices(OM, by_key(:obj))
    @test joinindices(OM, by_key(:obj); loop_over_side=1, nonmatches=keep) != joinindices(OM, by_key(:obj); loop_over_side=2, nonmatches=keep)
    test_unique_setequal(joinindices(OM, by_key(:obj); loop_over_side=1, nonmatches=keep), joinindices(OM, by_key(:obj); nonmatches=keep))
    test_unique_setequal(joinindices(OM, by_key(:obj); loop_over_side=2, nonmatches=keep), joinindices(OM, by_key(:obj); nonmatches=keep))

    @test joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)); loop_over_side=2) == joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)))
    @test joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)); loop_over_side=:M) == joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)))
    @test_throws ErrorException joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)); loop_over_side=1)
    @test_throws ErrorException joinindices(OM, by_pred(:obj, ∈, x -> (x.obj,)); loop_over_side=:O)
end

@testitem "unnested" begin
    using StructArrays

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

    @testset begin
        J1 = innerjoin((O=objects, M1=measurements), by_key(:obj))

        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test J2.J.O == J3.O
        @test J2.J.M1 == J3.M1
        @test J2.M2 == J3.M2
        @test innerjoin((__=J1, M2=measurements), by_key(:obj ∘ :O, :obj)) == J3
        @test_broken innerjoin((__=J1, M2=measurements), by_key(O=:obj, M2=:obj)) == J3  # should this work?
        @test_broken innerjoin((__=J1, M2=measurements), by_key(O=:obj, M2=:obj); groupby=:O)  # should this work?
        @test_broken innerjoin((__=J1, M2=measurements), by_key(__=x->x.o.obj, M2=:obj); groupby=:O)  # should this work?

        J4 = innerjoin((_=J1, __=StructArray(measurements)), by_key(:obj ∘ :O, :obj))
        @test J2.J.O == J4.O
        @test J2.J.M1 == J4.M1
        @test map(x -> x.obj, J2.M2) == J4.obj
        @test map(x -> x.time, J2.M2) == J4.time

        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj); groupby=:M2)
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj); groupby=:M2)
        @test map(r -> r.O, J2.J) == J3.O
        @test map(r -> r.M1, J2.J) == J3.M1
        @test J2.M2 == J3.M2
    end

    @testset begin
        J1 = innerjoin((O=objects, M1=measurements), by_key(:obj); groupby=:O)
        J2 = innerjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = innerjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test J2.J.O == J3.O
        @test J2.J.M1 == J3.M1
        @test J2.M2 == J3.M2
    end

    @testset "non-innerjoin" begin
        J1 = innerjoin((O=objects, M1=measurements), by_key(:obj); groupby=:O)
        J2 = leftjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = leftjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test J2.J.O == J3.O
        @test J2.J.M1 == J3.M1
        @test J2.M2 == J3.M2

        J2 = rightjoin((J=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        J3 = rightjoin((_=J1, M2=measurements), by_key(:obj ∘ :O, :obj))
        @test map(r -> isnothing(r) ? nothing : r.O, J2.J) == J3.O
        @test map(r -> isnothing(r) ? nothing : r.M1, J2.J) == J3.M1
        @test J2.M2 == J3.M2
    end
end

@testitem "types" begin
    using StructArrays

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

    @testset "container" begin
        @testset "basic" begin
            J = innerjoin(OM, by_key(:obj))
            @test J isa StructArray
            @test J.O isa SubArray
            @test J.M isa SubArray
            Jm = FlexiJoins.materialize_views(J)
            @test Jm isa StructArray
            @test Jm.O isa Vector{<:NamedTuple}
            @test Jm.M isa Vector{<:NamedTuple}
        end

        @testset "grouped" begin
            J = innerjoin(OM, by_key(:obj); groupby=:O)
            @test J isa StructArray
            @test J.O isa SubArray
            @test J.M isa Vector{<:SubArray}
            Jm = FlexiJoins.materialize_views(J)
            @test Jm isa StructArray
            @test Jm.O isa Vector{<:NamedTuple}
            @test Jm.M isa Vector{<:Vector}
        end

        @testset "sentinel" begin
            J = leftjoin(OM, by_key(:obj))
            @test J isa StructArray
            @test J.O isa SubArray
            @test J.M isa FlexiJoins.SentinelView
            Jm = FlexiJoins.materialize_views(J)
            @test Jm isa StructArray
            @test Jm.O isa Vector{<:NamedTuple}
            @test Jm.M isa Vector{<:Union{Nothing, NamedTuple}}
        end

        @testset "structarray" begin
            J = innerjoin((O=StructArray(objects), M=StructArray(measurements)), by_key(:obj))
            @test J isa StructArray
            @test J.O isa StructArray
            @test J.M isa StructArray
            @test J.O.obj isa SubArray
            @test J.M.obj isa SubArray

            Jm = FlexiJoins.materialize_views(J)
            @test Jm isa StructArray
            @test Jm.O isa StructArray
            @test Jm.M isa StructArray
            @test Jm.O.obj isa Vector{String}
            @test Jm.M.obj isa Vector{String}
        end

        @testset "structarray grouped" begin
            J = innerjoin((O=StructArray(objects), M=StructArray(measurements)), by_key(:obj); groupby=:O)
            @test J isa StructArray
            @test J.O isa StructArray
            @test J.M isa Vector  # can be a view?..
            @test J.O.obj isa SubArray
            @test J.M[1] isa StructArray
            @test J.M[1].obj isa SubArray

            Jm = FlexiJoins.materialize_views(J)
            @test Jm isa StructArray
            @test Jm.O isa StructArray
            @test Jm.M isa Vector
            @test Jm.O.obj isa Vector{String}
            @test Jm.M[1] isa StructArray
            @test Jm.M[1].obj isa Vector{String}
        end
    end

    @testset "eltype" begin
        J = innerjoin(OM, by_key(:obj))
        @test eltype(J.O) == eltype(objects)
        @test eltype(J.M) == eltype(measurements)
        J = leftjoin(OM, by_key(:obj))
        @test eltype(J.O) == eltype(objects)
        @test eltype(J.M) == Union{Nothing, eltype(measurements)}
        J = rightjoin(OM, by_key(:obj))
        @test eltype(J.O) == Union{Nothing, eltype(objects)}
        @test eltype(J.M) == eltype(measurements)
        J = outerjoin(OM, by_key(:obj))
        @test eltype(J.O) == Union{Nothing, eltype(objects)}
        @test eltype(J.M) == Union{Nothing, eltype(measurements)}
    end
end

@testitem "cardinality" begin
    OM = (;O=["A", "B", "D", "E"], M=["A", "A", "B", "B", "C", "C", "C"])

    J = joinindices(OM, by_key(identity))
    @test joinindices(OM, by_key(identity); cardinality=*) == J
    @test joinindices(OM, by_key(identity); cardinality=(M=*,)) == J
    @test joinindices(OM, by_key(identity); cardinality=(M=0:4,)) == J
    @test joinindices(OM, by_key(identity); cardinality=(O=0:1,)) == J
    @test joinindices(OM, by_key(identity); cardinality=(M=0:4, O=0:1)) == J
    @test joinindices(OM, by_key(identity); cardinality=(0:1, 0:4)) == J
    @test_throws "exceeded: got 2, expected 1" joinindices(OM, by_key(identity); cardinality=(O=1, M=1))
    @test_throws "exceeded: got 1, expected 0" joinindices(OM, by_key(identity); cardinality=(O=*, M= ==(0)))
    @test_throws "exceeded: got 1, expected 0" joinindices(OM, by_key(identity); cardinality=(O=0, M=*))
    @test_throws "mismatch: got 0, expected >(0)" joinindices(OM, by_key(identity); cardinality=(O=*, M=+))
    @test_throws "mismatch: got 0, expected >(0)" joinindices(OM, by_key(identity); cardinality=(O = >(0), M=*))
    @test_throws "mismatch: got 0, expected >(0)" joinindices(OM, by_key(identity); cardinality=(O = >(0), M=+))
    @test_throws "mismatch: got 0, expected >(0)" joinindices(OM, by_key(identity); cardinality=(M = >(0),))
    @test_throws "exceeded: got 2, expected 0:1" joinindices(OM, by_key(identity); cardinality=(M=0:1,))
    @test_throws "mismatch: got 0, expected 1:3" joinindices(OM, by_key(identity); cardinality=(O=1:3,))

    J = joinindices((;O=OM.O, O2=OM.O), by_key(identity))
    @test joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=1) == J
    @test joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=(1, 1)) == J
    @test joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=(1, +)) == J
    @test joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=(O=+,)) == J
    @test_throws "exceeded: got 1, expected 0" joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=(1, 0))
    @test_throws "mismatch: got 1, expected 2:100" joinindices((;O=OM.O, O2=OM.O), by_key(identity); cardinality=(1, 2:100))

    J = joinindices((;M=OM.M, M2=OM.M), by_key(identity))
    @test joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=2:3) == J
    @test joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=(2:4, 2:3)) == J
    @test joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=(2:3, +)) == J
    @test joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=(M=+,)) == J
    @test_throws "exceeded: got 1, expected 0" joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=(1, 0))
    @test_throws "mismatch: got 2, expected 3:100" joinindices((;M=OM.M, M2=OM.M), by_key(identity); cardinality=(*, 3:100))
end

@testitem "show" begin
    using Accessors
    using Distances: Euclidean

    @test string(by_key(@o(_.a[12]), :b) & by_key(:key) & by_pred(:id, <, :id1) & by_distance(:time, Euclidean(), <=(3)) & not_same()) ==
        "by_key((@o _.a[12]) == (@o _.b)) & by_key((@o _.key)) & by_pred((@o _.id) < (@o _.id1)) & by_distance(Distances.Euclidean(0.0)((@o _.time), (@o _.time)) <= 3.0) & not_same(order_matters=true)"
end

@testitem "join modes" begin
    using FlexiJoins: Mode
    using Accessors
    using IntervalSets
    using Distances: Euclidean
    using StaticArrays: SVector

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
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
                allocs = @allocations joinindices(LR, Base.tail(args)...; kwargs..., mode)
                if cond isa FlexiJoins.ByDistance
                    @test_broken allocs < 150
                else
                    @test allocs < 150
                end
            end
        end
    end

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    OM = (;O=objects, M=measurements)

    @testset "$cond" for (cond, modes, kwargs) in [
            (by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
            (by_key(x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()], (;)),
            (by_distance(:value, :time, Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
            (by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
            (by_pred(:obj, ==, :obj), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
            (by_pred(:obj, ==, x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()], (;)),
            (by_pred(:value, <, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, <=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, >, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, >=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> (x.time, x.time, x.time + 5, x.time + 10)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;alloc=false)),
            (by_pred(:value, ∈, x -> x.time..(x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> Interval{:open,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> Interval{:closed,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> Interval{:open,:closed}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(x -> (x.value-5)..(x.value+4), ⊇, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Sort()], (;alloc=false)),
            (by_pred(x -> (x.value-1)..(x.value+2), (!) ∘ isdisjoint, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Tree()], (;alloc=false)),
            (by_key(@optic(_.obj)) & by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort()], (;)),
        ]
        test_modes(modes, OM, cond; kwargs...)
        test_modes(modes, (;O=objects[1:0], M=measurements), cond; kwargs...)
        test_modes(modes, (;O=objects, M=measurements[1:0]), cond; kwargs...)
        test_modes(modes, (;O=objects[1:0], M=measurements[1:0]), cond; kwargs...)
        test_modes(modes, OM, cond; nonmatches=(O=keep,), kwargs...)
        test_modes(modes, OM, cond; nonmatches=keep, kwargs...)

        first_M = cond isa FlexiJoins.ByPred{typeof(∈)}  # the ∈ condition only supports a single "direction"

        @testset "cache" begin
            base = joinindices(OM, cond)
            cache = join_cache()
            @test isnothing(cache.prepared)
            test_unique_setequal(joinindices(OM, cond; cache), base)
            @test !isnothing(cache.prepared)
            test_unique_setequal(joinindices(OM, cond; cache), base)
            @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache)
            @test_throws AssertionError joinindices(OM, by_key(:abc); cache)
            @test_throws AssertionError joinindices(OM, cond; multi=first_M ? (O=first,) : (M=first,), cache)
            @test_throws AssertionError joinindices(OM, cond; mode=Mode.NestedLoop(), cache)

            if !first_M
                cache = join_cache()
                @test isnothing(cache.prepared)
                test_unique_setequal(joinindices(OM, cond; cache, loop_over_side=:O), base)
                @test !isnothing(cache.prepared)
                test_unique_setequal(joinindices((;O=copy(objects), M=measurements), cond; cache, loop_over_side=:O), base)
                @test_throws AssertionError joinindices((;O=objects, M=copy(measurements)), cond; cache, loop_over_side=:O)
                @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache, loop_over_side=:O)
                @test_throws r"AssertionError: cache\.params|No known mode supported" joinindices(OM, cond; cache, loop_over_side=:M)
            end
        end

        test_modes(modes, OM, cond; multi=first_M ? (O=first,) : (M=first,), kwargs...)

        # order within groups may differ, so tests fail:
        # test_modes(modes, OM, cond; groupby=:O)
        # test_modes(modes, OM, cond; groupby=:O, nonmatches=keep)
        for mode in [nothing; modes]
            # smoke test
            joinindices(OM, cond; groupby=first_M ? :M : :O, mode)
        end
    end

    test_modes([Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], OM, by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,))
    test_modes([Mode.NestedLoop(), Mode.Sort()], OM, by_pred(:value, <, :time); multi=(M=closest,))
    test_modes([Mode.NestedLoop(), Mode.Hash()], (measurements, measurements), by_key(:obj) & not_same(); alloc=false)
    test_modes([Mode.NestedLoop(), Mode.NestedLoopFast()], (measurements, measurements), not_same(); alloc=false)
end

@testitem "random" begin
    using FlexiJoins: Mode
    using Accessors
    using IntervalSets
    using Distances: Euclidean
    using StaticArrays: SVector
    using Random

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
    end

    function test_modes(modes, args...; alloc=true, kwargs...)
        base = joinindices(args...; kwargs..., mode=Mode.NestedLoop())
        @testset for mode in [nothing; modes]
            cur = joinindices(args...; kwargs..., mode)
            test_unique_setequal(cur, base)
        end
    end

    for i in 1:5
        Random.seed!(i)
        objects = [(obj=randstring('a':'z', 1), value=rand(1:10)) for _ in 1:rand(1:100)]
        measurements = [(obj, time=t) for (obj, cnt) in [(randstring('a':'z', 1), rand(1:10)) for _ in 1:rand(1:20)] for t in cnt .* (2:(cnt+1))]
        OM = (;O=objects, M=measurements)

        @testset "$cond" for (cond, modes, kwargs) in [
                (by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
                (by_key(x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()], (;)),
                (by_distance(:value, :time, Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
                (by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
                (by_pred(:obj, ==, :obj), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
                (by_pred(:obj, ==, x -> x.obj == "B" ? nothing : x.obj), [Mode.NestedLoop(), Mode.Hash()], (;)),
                (by_pred(:value, <, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, <=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, >, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, >=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, ∈, x -> (x.time, x.time, x.time + 5, x.time + 10)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;alloc=false)),
                (by_pred(:value, ∈, x -> x.time..(x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, ∈, x -> Interval{:open,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, ∈, x -> Interval{:closed,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(:value, ∈, x -> Interval{:open,:closed}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_pred(x -> (x.value-5)..(x.value+4), ⊇, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Sort()], (;alloc=false)),
                (by_pred(x -> (x.value-1)..(x.value+2), (!) ∘ isdisjoint, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Tree()], (;alloc=false)),
                (by_key(@optic(_.obj)) & by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()], (;)),
                (by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)) & by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            ]
            test_modes(modes, OM, cond; kwargs...)
            test_modes(modes, (;O=objects[1:0], M=measurements), cond; kwargs...)
            test_modes(modes, (;O=objects, M=measurements[1:0]), cond; kwargs...)
            test_modes(modes, (;O=objects[1:0], M=measurements[1:0]), cond; kwargs...)
            test_modes(modes, OM, cond; nonmatches=(O=keep,), kwargs...)
            test_modes(modes, OM, cond; nonmatches=keep, kwargs...)

            first_M = cond isa FlexiJoins.ByPred{typeof(∈)}  # the ∈ condition only supports a single "direction"

            @testset "cache" begin
                base = joinindices(OM, cond)
                cache = join_cache()
                @test isnothing(cache.prepared)
                test_unique_setequal(joinindices(OM, cond; cache), base)
                @test !isnothing(cache.prepared)
                test_unique_setequal(joinindices(OM, cond; cache), base)
                @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache)
                @test_throws AssertionError joinindices(OM, by_key(:abc); cache)
                @test_throws AssertionError joinindices(OM, cond; multi=first_M ? (O=first,) : (M=first,), cache)
                @test_throws AssertionError joinindices(OM, cond; mode=Mode.NestedLoop(), cache)

                if !first_M
                    cache = join_cache()
                    @test isnothing(cache.prepared)
                    test_unique_setequal(joinindices(OM, cond; cache, loop_over_side=:O), base)
                    @test !isnothing(cache.prepared)
                    test_unique_setequal(joinindices((;O=copy(objects), M=measurements), cond; cache, loop_over_side=:O), base)
                    @test_throws AssertionError joinindices((;O=objects, M=copy(measurements)), cond; cache, loop_over_side=:O)
                    @test_throws AssertionError joinindices((;O=copy(objects), M=copy(measurements)), cond; cache, loop_over_side=:O)
                    @test_throws r"AssertionError: cache\.params|No known mode supported" joinindices(OM, cond; cache, loop_over_side=:M)
                end
            end

            test_modes(modes, OM, cond; multi=first_M ? (O=first,) : (M=first,), kwargs...)

            # order within groups may differ, so tests fail:
            # test_modes(modes, OM, cond; groupby=:O)
            # test_modes(modes, OM, cond; groupby=:O, nonmatches=keep)
            for mode in [nothing; modes]
                # smoke test
                joinindices(OM, cond; groupby=first_M ? :M : :O, mode)
            end
        end

        # closest can be below or above: ambigous and results differ between modes
        # test_modes([Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], OM, by_distance(:value, :time, Euclidean(), <=(3)); multi=(M=closest,))
        # test_modes([Mode.NestedLoop(), Mode.Sort()], OM, by_pred(:value, <, :time); multi=(M=closest,))
        test_modes([Mode.NestedLoop(), Mode.Hash()], (measurements, measurements), by_key(:obj) & not_same(); alloc=false)
        test_modes([Mode.NestedLoop(), Mode.NestedLoopFast()], (measurements, measurements), not_same(); alloc=false)
    end
end

@testitem "weird values" begin
    using FlexiJoins: Mode
    using Distances: Euclidean
    using StaticArrays
    using IntervalSets
    using Accessors

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
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
                allocs = @allocations joinindices(LR, Base.tail(args)...; kwargs..., mode)
                if cond isa FlexiJoins.ByDistance
                    @test_broken allocs < 150
                else
                    @test allocs < 150
                end
            end
        end
    end

    objects = [(obj="A", value=2.), (obj=missing, value=-5.), (obj="D", value=0.0), (obj="E", value=-0.0)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in [NaN; cnt .* (-cnt:(cnt+1))]]
    OM = (;O=objects, M=measurements)

    @testset "$cond" for (cond, modes, kwargs) in [
            (by_key(@optic(_.obj)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
            # Sort differs:
            # (by_distance(:value, :time, Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
            (by_distance(x -> SVector(0, x.value), x -> SVector(0, x.time), Euclidean(), <=(3)), [Mode.NestedLoop(), Mode.Sort(), Mode.Tree()], (;)),
            (by_pred(:obj, isequal, :obj), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;)),
            # Hash mode differs:
            # (by_pred(:value, ==, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, isequal, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            # NaNs in the end:
            # (by_pred(:value, <, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            # (by_pred(:value, <=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, >, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            # (by_pred(:value, >=, :time), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(x -> x.value..(x.value + 10), ∋, @optic(_.time)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            # (by_pred(:value, ∈, x -> (x.time, x.time, x.time + 5, x.time + 10)), [Mode.NestedLoop(), Mode.Sort(), Mode.Hash()], (;alloc=false)),
            # (by_pred(:value, ∈, x -> x.time..(x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> Interval{:open,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            # (by_pred(:value, ∈, x -> Interval{:closed,:open}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(:value, ∈, x -> Interval{:open,:closed}(x.time, x.time + 10)), [Mode.NestedLoop(), Mode.Sort()], (;)),
            (by_pred(x -> (x.value-5)..(x.value+4), ⊇, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Sort()], (;alloc=false)),
            # (by_pred(x -> (x.value-1)..(x.value+2), (!) ∘ isdisjoint, x -> (x.time-1)..(x.time+2)), [Mode.NestedLoop(), Mode.Tree()], (;)),
        ]
        test_modes(modes, OM, cond; alloc=false)
    end
end


@testitem "normalize_arg" begin
    using FlexiJoins: normalize_arg, ByKey
    using Accessors

    @test normalize_arg(by_key(@optic(_.obj)), (A=[], B=[])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj), (A=[], B=[])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj), ([], [])) == ByKey((@optic(_.obj), @optic(_.obj)))
    @test normalize_arg(by_key(:obj, @optic(_.name)), ([], [])) == ByKey((@optic(_.obj), @optic(_.name)))
    @test normalize_arg(by_key(A=@optic(_.name), B=:obj), (A=[], B=[])) == ByKey((@optic(_.name), @optic(_.obj)))
end

@testitem "skycoords" begin
    using SkyCoords
    using FlexiJoins: Mode
    using Accessors

    function test_unique_setequal(a, b)
        @test allunique(a)
        @test allunique(b)
        @test issetequal(a, b)
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
                allocs = @allocations joinindices(LR, Base.tail(args)...; kwargs..., mode)
                if cond isa FlexiJoins.ByDistance
                    @test_broken allocs < 150
                else
                    @test allocs < 150
                end
            end
        end
    end

    sides = (
        L=[ICRSCoords(0, 0), ICRSCoords(0, 0.5), ICRSCoords(0, 1), ICRSCoords(0.5, 0), ICRSCoords(0.5, 0.5), ICRSCoords(0.5, 1), ICRSCoords(1, 0), ICRSCoords(1, 0.5), ICRSCoords(1, 1)],
        R=[ICRSCoords(0, 0.68), ICRSCoords(0, 0.7), ICRSCoords(0, 0.72), ICRSCoords(4, 0), ICRSCoords(4, 0.5), ICRSCoords(4, 1), ICRSCoords(4.5, 0), ICRSCoords(5.5, 0.5), ICRSCoords(6, 1), ICRSCoords(6, 0), ICRSCoords(6, 0.5), ICRSCoords(6, 1)],
    )
    sides = @modify(coo -> (; coo), sides.L |> Elements())
    test_modes([Mode.NestedLoop(), Mode.Tree()], sides, by_distance(:coo, identity, separation, <=(0.7)))
    sides = (
        L=[ICRSCoords(0, 0), ICRSCoords(0, 0.5), ICRSCoords(0, 1), ICRSCoords(0.5, 0), ICRSCoords(0.5, 0.5), ICRSCoords(0.5, 1), ICRSCoords(1, 0), ICRSCoords(1, 0.5), ICRSCoords(1, 1)],
        R=[GalCoords(4, 0), GalCoords(4, 0.5), GalCoords(4, 1), GalCoords(4.5, 0), GalCoords(5.5, 0.5), GalCoords(6, 1), GalCoords(6, 0), GalCoords(6, 0.5), GalCoords(6, 1)],
    )
    sides = @modify(coo -> (; coo), sides.R |> Elements())
    test_modes([Mode.NestedLoop(), Mode.Tree()], sides, by_distance(identity, :coo, separation, <=(0.7)))
end

@testitem "other dataset types" begin
    using FlexiJoins: Mode
    using DataPipes
    using StructArrays
    using TypedTables: Table
    using OffsetArrays
    using Dictionaries
    using DataFrames

    # https://github.com/JuliaLang/julia/pull/49179
    Base.keytype(@nospecialize t::Tuple) = keytype(typeof(t))
    Base.keytype(@nospecialize T::Type{<:Tuple}) = Int

    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1), (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    expected = [((obj="A", value=2), (obj="A", time=8)), ((obj="A", value=2), (obj="A", time=12)), ((obj="A", value=2), (obj="A", time=16)), ((obj="A", value=2), (obj="A", time=20)), ((obj="B", value=-5), (obj="B", time=2))]

    @testset for mode in [nothing, Mode.NestedLoop(), Mode.Sort(), Mode.Hash()]
        @testset "tuple" begin
            @test flexijoin((Tuple(objects), Tuple(measurements)), by_key(:obj); mode) == expected
        end

        @testset "pairs" begin
            @test_broken flexijoin((objects, pairs(measurements)), by_key(:obj, x -> x[2].obj); mode) == expected
            @test_broken flexijoin((pairs(objects), measurements), by_key(x -> x[2].obj, :obj); mode) == expected
            @test_broken flexijoin((pairs(objects), pairs(measurements)), by_key(x -> x[2].obj); mode) == expected
        end

        @testset "structarray" begin
            @test flexijoin((objects |> StructArray, measurements |> StructArray), by_key(:obj); mode) == expected
        end

        @testset "typedtable" begin
            @test flexijoin((objects |> Table, measurements |> Table), by_key(:obj); mode) == expected
        end

        @testset "offsetarray" begin
            @test flexijoin((OffsetArray(objects, -100), measurements), by_key(:obj); mode) == expected
            @test flexijoin((objects, OffsetArray(measurements, 1000)), by_key(:obj); mode) == expected
            @test flexijoin((OffsetArray(objects, -100), OffsetArray(measurements, 1000)), by_key(:obj); mode) == expected
        end

        @testset "dictionary" begin
            if mode != Mode.Sort()
                @test flexijoin((objects, dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj); mode) == expected
                @test flexijoin((dictionary(string.('w':'z') .=> objects), measurements), by_key(:obj); mode) == expected
                @test flexijoin((dictionary(string.('w':'z') .=> objects), dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj); mode) == expected
            else
                @test flexijoin((objects, dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj); mode) == expected
                @test_broken flexijoin((dictionary(string.('w':'z') .=> objects), measurements), by_key(:obj); mode) == expected
                @test_broken flexijoin((dictionary(string.('w':'z') .=> objects), dictionary(Symbol.('a':'h') .=> measurements)), by_key(:obj); mode) == expected
            end
        end

        @testset "dataframe" begin
            odf = DataFrame(objects)
            mdf = DataFrame(measurements)
            edf = @p expected |> map((;_[1]..., obj_1=_[2].obj, _[2].time)) |> DataFrame
            @test flexijoin((odf, mdf), by_key(:obj); mode) == edf
            @test isequal(
                leftjoin((odf, mdf), by_key(:obj); mode),
                vcat(edf, DataFrame([(obj="D", value=1, obj_1=missing, time=missing), (obj="E", value=9, obj_1=missing, time=missing)]))
            )
        end
    end
end

@testitem "_" begin
    import CompatHelperLocal as CHL
    CHL.@check()

    import Aqua
    Aqua.test_all(FlexiJoins; ambiguities=false, piracies=false)
    Aqua.test_ambiguities(FlexiJoins)
end
