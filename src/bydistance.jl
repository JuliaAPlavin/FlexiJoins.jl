function closest end

struct ByDistance{TFL, TFR, TD, TP <: Union{typeof.((<, <=))...}} <: JoinCondition
    func_L::TFL
    func_R::TFR
    dist::TD
    pred::TP
    max::Float64
end

by_distance(func, dist, maxpred::Base.Fix2) = ByDistance(func, func, dist, maxpred.f, Float64(maxpred.x))
by_distance(func_L, func_R, dist, maxpred::Base.Fix2) = ByDistance(func_L, func_R, dist, maxpred.f, Float64(maxpred.x))

normalize_arg(cond::ByDistance, datas) = cond

supports_mode(::Mode.NestedLoop, ::ByKey, datas) = true
is_match(by::ByDistance, a, b) = by.pred(by.dist(by.func_L(a), by.func_R(b)), by.max)


supports_mode(::Mode.Sort, ::ByDistance, datas) = true
sort_byf(which, cond::ByDistance) = x -> first(which((cond.func_L, cond.func_R))(x))
function searchsorted_matchix(cond::ByDistance, a, B, perm)
    arr = mapview(i -> first(cond.func_R(B[i])), perm)
    val = cond.func_L(a)
    P = @view perm[searchsortedfirst(arr, val - cond.max):searchsortedlast(arr, val + cond.max)]
    return filter(i -> is_match(cond, a, B[i]), P)
end
