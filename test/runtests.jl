using FlexiJoins
using Test

@testset begin
    objects = [(obj="A", value=2), (obj="B", value=-5), (obj="D", value=1),  (obj="E", value=9)]
    measurements = [(obj, time=t) for (obj, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]
    @test flexijoin((;O=objects, M=measurements), by_key(@optic(_.obj))) == [(O = (obj = "A", value = 2), M = (obj = "A", time = 8)), (O = (obj = "A", value = 2), M = (obj = "A", time = 12)), (O = (obj = "A", value = 2), M = (obj = "A", time = 16)), (O = (obj = "A", value = 2), M = (obj = "A", time = 20)), (O = (obj = "B", value = -5), M = (obj = "B", time = 2))]
end


import CompatHelperLocal as CHL
CHL.@check()
