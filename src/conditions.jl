is_match(by, a) = b -> is_match(by, a, b)

abstract type JoinCondition end

struct CompositeCondition{TC} <: JoinCondition
    conds::TC
end

is_match(by::CompositeCondition, a, b) = all(by1 -> is_match(by1, a, b), by.conds)

Base.:(&)(a::JoinCondition, b::JoinCondition) = CompositeCondition((a, b))
Base.:(&)(a::CompositeCondition, b::JoinCondition) = CompositeCondition((a.conds..., b))
Base.:(&)(a::JoinCondition, b::CompositeCondition) = CompositeCondition((a, b.conds))
Base.:(&)(a::CompositeCondition, b::CompositeCondition) = CompositeCondition((a.conds..., b.conds...))

# function extra(E::Val, by::CompositeCondition, args...)
# 	cond = filter(c -> applicable(extra, E, c, args...), by.conds) |> only
# 	return extra(E, cond, args...)
# end

struct ByKey{TF} <: JoinCondition
    keyfunc::TF
end

function index end
by_key(keyfunc::typeof(index)) = ByKey(only ∘ parentindices)
by_key(keyfunc) = ByKey(@optic(keyfunc(_[])))

is_match(by::ByKey, a, b) = by.keyfunc(a) == by.keyfunc(b)

# extra(::Val{:key}, by::ByKey, a::Nothing, b) = by.keyfunc(b)
# extra(::Val{:key}, by::ByKey, a, b::Nothing) = by.keyfunc(a)
# extra(::Val{:key}, by::ByKey, a, b) = (@assert by.keyfunc(a) == by.keyfunc(b); by.keyfunc(a))

stripnames(cond::ByKey, datas::NamedTuple{NS}) where {NS} = ByKey(cond.keyfunc)
stripnames(cond::ByKey, datas::Tuple) = ByKey(cond.keyfunc)
reverse_sides(cond::ByKey) = ByKey(cond.keyfunc)

struct ByPred{TP} <: JoinCondition
    pred::TP
end

by_pred(pred) = ByPred(pred)
is_match(by::ByPred, a, b) = by.pred(a, b)

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
