using FlexiJoins
using FlexiJoins: NothingIndex
using Test


function test_unique_setequal(a, b)
    @test allunique(a)
    @test allunique(b)
    @test issetequal(a, b)
end


@testset "basic" begin
    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1),  (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    @test flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj))) ==
        [(O=(obj="A", value=2), M=(obj="A", time=8)), (O=(obj="A", value=2), M=(obj="A", time=12)), (O=(obj="A", value=2), M=(obj="A", time=16)), (O=(obj="A", value=2), M=(obj="A", time=20)), (O=(obj="B", value=-5), M=(obj="B", time=2))]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj))) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5)]

    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=(O=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=NothingIndex()), (O=4, M=NothingIndex())]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=(M=keep,)) ==
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=NothingIndex(), M=6), (O=NothingIndex(), M=7), (O=NothingIndex(), M=8)]
    test_unique_setequal(
        joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); nonmatches=keep),
        [(O=1, M=1), (O=1, M=2), (O=1, M=3), (O=1, M=4), (O=2, M=5), (O=3, M=NothingIndex()), (O=4, M=NothingIndex()), (O=NothingIndex(), M=6), (O=NothingIndex(), M=7), (O=NothingIndex(), M=8)]
    )

    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=first,)) ==
        [(O=1, M=1), (O=2, M=5)]
    @test joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); multi=(M=last,)) ==
        [(O=1, M=4), (O=2, M=5)]

    @test_throws AssertionError joinindices((;O=objects, M=measurements), by_key(@optic(_.obj)); cardinality=(O=1, M=1)) |> materialize_views
end


import CompatHelperLocal as CHL
CHL.@check()
