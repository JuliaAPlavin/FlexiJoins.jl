struct ByDistance{TFL, TFR, TD, TP <: Union{typeof.((<, <=))...}} <: JoinCondition
    func_L::TFL
    func_R::TFR
    dist::TD
    pred::TP
    max::Float64
end

Base.show(io::IO, c::ByDistance) = print(io, "by_distance(", c.dist, '(', c.func_L, ", ", c.func_R, ") ", c.pred, ' ', c.max, ")")

swap_sides(c::ByDistance) = ByDistance(c.func_R, c.func_L, c.dist, c.pred, c.max)

"""
    by_distance(f, dist, pred)
    by_distance(f_L, f_R, dist, pred)

Join condition with `left`-`right` matches defined by `pred(dist(f_L(left), f_R(left)))`.
All distances from `Distances.jl` are supported as `dist.`

# Examples

```
by_distance(:time, Euclidean(), <=(3))
by_distance(:time, x -> minimum(x.times), Euclidean(), <=(3))
```
"""
by_distance(func, dist, maxpred::Base.Fix2) = by_distance(func, func, dist, maxpred)
by_distance(func_L, func_R, dist, maxpred::Base.Fix2) = ByDistance(normalize_keyfunc(func_L), normalize_keyfunc(func_R), dist, maxpred.f, Float64(maxpred.x))

supports_mode(::Mode.NestedLoop, ::ByDistance, datas) = true
is_match(by::ByDistance, a, b) = by.pred(by.dist(by.func_L(a), by.func_R(b)), by.max)
findmatchix(mode::Mode.NestedLoop, cond::ByDistance, ix_a, a, B, multi::Closest) =
    @p findmatchix(mode, cond, ix_a, a, B, identity) |>
        firstn_by!(by=i -> cond.dist(cond.func_L(a), cond.func_R(B[i])))


supports_mode(::Mode.Sort, ::ByDistance, datas) = true
function sort_byf(cond::ByDistance)
    # check cond.dist isa NN.MinkowskiMetric, without depending on NN.jl:
    nameof(typeof(cond.dist)) ∈ (:Euclidean, :Chebyshev, :Cityblock, :Minkowski, :WeightedEuclidean, :WeightedCityblock, :WeightedMinkowski) ||
        @warn "Joining by distance using componentwise sorting, this doesn't work for all distance types" cond.dist
    x -> first(cond.func_R(x))
end
function searchsorted_matchix(cond::ByDistance, a, B, perm)
    arr = mapview(i -> first(cond.func_R(@inbounds B[i])), perm)
    val = cond.func_L(a)
    P = @view perm[searchsortedfirst(arr, first(val) - cond.max):searchsortedlast(arr, first(val) + cond.max)]
    return filter(i -> is_match(cond, a, @inbounds B[i]), P)
end
searchsorted_matchix_closest(cond::ByDistance, a, B, perm) =
    @p searchsorted_matchix(cond, a, B, perm) |>
        firstn_by!(by=i -> cond.dist(cond.func_L(a), cond.func_R(B[i])))


supports_mode(::Mode.Tree, ::ByDistance, datas) = true
# signature should be the same as in nearestneighbors.jl:
prepare_for_join(::Mode.Tree, X, cond::ByDistance) = error("Load NearestNeighbors.jl to use tree-based join conditions")
