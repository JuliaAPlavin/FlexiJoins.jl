struct ByDistance{TFL, TFR, TD, TP <: Union{typeof.((<, <=))...}, TM} <: JoinCondition
    func_L::TFL
    func_R::TFR
    dist::TD
    pred::TP
    max::TM
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
by_distance(func_L, func_R, dist, maxpred::Base.Fix2) = ByDistance(normalize_keyfunc(func_L), normalize_keyfunc(func_R), dist, maxpred.f, maxpred.x)

supports_mode(::Mode.NestedLoop, ::ByDistance, datas) = true
is_match(by::ByDistance, a, b) = by.pred(by.dist(by.func_L(a), by.func_R(b)), by.max)
closeness(cond::ByDistance, a, b) = cond.dist(cond.func_L(a), cond.func_R(b))


supports_mode(::Mode.Sort, ::ByDistance, datas) = true
function sort_byf(cond::ByDistance)
    # check cond.dist is an unweighted Minkowski metric, without depending on NN.jl
    # weighted metrics aren't included: with weights < 1, matches can be further than max in the sorted coordinate
    nameof(typeof(cond.dist)) ∈ (:Euclidean, :Chebyshev, :Cityblock, :Minkowski) ||
        @warn "Joining by distance using componentwise sorting, this doesn't work for all distance types" cond.dist maxlog=1
    x -> sort_coord(cond.func_R(x))
end
function searchsorted_matchix(cond::ByDistance, a, B, perm)
    sf = sort_byf(cond)
    arr = mapview(i -> sf(@inbounds B[i]), perm)
    val = sort_coord(cond.func_L(a))
    P = @view perm[searchsortedfirst(arr, val - cond.max):searchsortedlast(arr, val + cond.max)]
    return filter(i -> is_match(cond, a, @inbounds B[i]), P)
end
searchsorted_matchix_closest(cond::ByDistance, a, B, perm) =
    @p searchsorted_matchix(cond, a, B, perm) |>
        firstn_by!(by=i -> cond.dist(cond.func_L(a), cond.func_R(B[i])))

# the coordinate that Sort mode sorts and searches by
sort_coord(x) = x
sort_coord(x::Union{AbstractVector, Tuple}) = first(x)


supports_mode(::Mode.Tree, ::ByDistance, datas) = true
