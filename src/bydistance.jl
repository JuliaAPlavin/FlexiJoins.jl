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
function sort_byf(which, cond::ByDistance)
    @warn "Joining by distance using componentwise sorting, this doesn't work for all distance types" cond.dist
    x -> first(which((cond.func_L, cond.func_R))(x))
end
function searchsorted_matchix(cond::ByDistance, a, B, perm)
    arr = mapview(i -> first(cond.func_R(B[i])), perm)
    val = cond.func_L(a)
    P = @view perm[searchsortedfirst(arr, val - cond.max):searchsortedlast(arr, val + cond.max)]
    return filter(i -> is_match(cond, a, B[i]), P)
end


supports_mode(::Mode.Tree, ::ByDistance, datas) = true
function prepare_for_join(::Mode.Tree, which, datas, cond::ByDistance, multi)
    X = which(datas)
    (X, NN.BallTree(map(which((cond.func_L, cond.func_R)), X) |> wrap_matrix, cond.dist))
end
findmatchix(::Mode.Tree, cond::ByDistance, a, (B, tree)::Tuple, multi::typeof(identity)) =
    NN.inrange(tree, wrap_vector(cond.func_L(a)), cond.max)
findmatchix(::Mode.Tree, cond::ByDistance, a, (B, tree)::Tuple, multi::typeof(closest)) =
    NN.inrange(tree, wrap_vector(cond.func_L(a)), cond.max)

wrap_matrix(X::Vector{<:AbstractVector}) = X
wrap_matrix(X::Vector{<:AbstractFloat}) = reshape(X, (1, :))
wrap_matrix(X::Vector{<:Integer}) = wrap_matrix(map(float, X))

wrap_vector(X::Vector{<:Number}) = X
wrap_vector(X::Number) = [X]
