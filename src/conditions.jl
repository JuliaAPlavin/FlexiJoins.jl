abstract type JoinCondition end

module Mode
struct Hash end
struct SortChain end
struct Sort end
struct NestedLoop end
end

supports_mode(mode, cond, datas) = false
supports_mode(::Mode.Sort, cond, datas) = supports_mode(Mode.SortChain(), cond, datas)

best_mode(cond, datas) =
    supports_mode(Mode.Hash(), cond, datas) ? Mode.Hash() :
    supports_mode(Mode.Sort(), cond, datas) ? Mode.Sort() :
    error("No known mode supported by $cond")


prepare_for_join(::Mode.NestedLoop, which, datas, cond::JoinCondition, multi) = which(datas)
findmatchix(::Mode.NestedLoop, cond::JoinCondition, a, B, multi) = propagate_empty(multi, findall(b -> is_match(cond, a, b), B))
propagate_empty(func::typeof(identity), arr) = arr
propagate_empty(func::Union{typeof.((first, last))...}, arr) = func(arr, 1)


function prepare_for_join(::Mode.Sort, which, datas, cond::JoinCondition, multi)
    X = which(datas)
    (X, sortperm(X; by=sort_byf(which, cond)))
end

findmatchix(::Mode.Sort, cond::JoinCondition, a, (B, perm)::Tuple, multi) = propagate_empty(multi, searchsorted_matchix(cond, a, B, perm)) |> collect


struct CompositeCondition{TC} <: JoinCondition
    conds::TC
end

is_match(by::CompositeCondition, a, b) = all(by1 -> is_match(by1, a, b), by.conds)

Base.:(&)(a::JoinCondition, b::JoinCondition) = CompositeCondition((a, b))
Base.:(&)(a::CompositeCondition, b::JoinCondition) = CompositeCondition((a.conds..., b))
Base.:(&)(a::JoinCondition, b::CompositeCondition) = CompositeCondition((a, b.conds))
Base.:(&)(a::CompositeCondition, b::CompositeCondition) = CompositeCondition((a.conds..., b.conds...))

normalize_arg(cond::CompositeCondition, datas) = CompositeCondition(map(c -> normalize_arg(c, datas), cond.conds))

supports_mode(mode::Mode.NestedLoop, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.SortChain, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.Sort, cond::CompositeCondition, datas) =
    all(c -> supports_mode(Mode.SortChain(), c, datas), cond.conds[1:end-1]) && supports_mode(mode, last(cond.conds), datas)

sort_byf(which, cond::CompositeCondition) = x -> map(c -> sort_byf(which, c)(x), cond.conds)

function searchsorted_matchix(cond::CompositeCondition, a, B, perm)
    perm = searchsorted_matchix(cond.conds[1], a, B, perm)
    perm = searchsorted_matchix(cond.conds[2], a, B, perm)
end



function closest end

struct ByDistance{TF, TD, TP} <: JoinCondition
    func::TF
    dist::TD
    pred::TP
    max::Float64
end

by_distance(func, max) = by_distance(func, Euclidean(), max)
by_distance(func, dist, max::Real) = ByDistance(func, dist, <=, Float64(max))
by_distance(func, dist, maxpred::Base.Fix2) = ByDistance(func, dist, maxpred.f, Float64(maxpred.x))

is_match(by::ByDistance, a, b) = by.pred(by.dist(by.func(a), by.func(b)), by.max)

# extra(::Val{:distance}, by::ByDistance, a, b) = by.dist(by.func(a), by.func(b))
# extra(::Val{:distance}, by::ByDistance, a::Nothing, b) = nothing
# extra(::Val{:distance}, by::ByDistance, a, b::Nothing) = nothing
