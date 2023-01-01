struct Closest end
const closest = Closest()


abstract type JoinCondition end


module Mode
struct Hash end
struct SortChain end
struct Sort end
struct Tree end
struct NestedLoop end
end

supports_mode(mode, cond, datas) = false
supports_mode(::Mode.Sort, cond, datas) = supports_mode(Mode.SortChain(), cond, datas)

best_mode(cond, datas) =
    supports_mode(Mode.Hash(), cond, datas) ? Mode.Hash() :
    supports_mode(Mode.Tree(), cond, datas) ? Mode.Tree() :
    supports_mode(Mode.Sort(), cond, datas) ? Mode.Sort() :
    error("No known mode supported by $cond")


findmatchix(mode, cond::JoinCondition, a, B_prep, multi::typeof(first)) = propagate_empty(minimum, findmatchix(mode, cond, a, B_prep, identity))
findmatchix(mode, cond::JoinCondition, a, B_prep, multi::typeof(last)) = propagate_empty(maximum, findmatchix(mode, cond, a, B_prep, identity))

prepare_for_join(::Mode.NestedLoop, which, datas, cond::JoinCondition, multi) = which(datas)
findmatchix(::Mode.NestedLoop, cond::JoinCondition, a, B, multi::typeof(identity)) = findall(b -> is_match(cond, a, b), B)
propagate_empty(func::typeof(identity), arr) = arr
propagate_empty(func::Union{typeof.((first, last))...}, arr) = func(arr, 1)
propagate_empty(func::Union{typeof.((minimum, maximum))...}, arr) = isempty(arr) ? arr : [func(arr)]


function prepare_for_join(::Mode.Sort, which, datas, cond::JoinCondition, multi)
    X = which(datas)
    (X, sortperm(X; by=sort_byf(which, cond)))
end

findmatchix(::Mode.Sort, cond::JoinCondition, a, (B, perm)::Tuple, multi::typeof(identity)) =
    searchsorted_matchix(cond, a, B, perm) |> collect  # sort to keep same order?
findmatchix(::Mode.Sort, cond::JoinCondition, a, (B, perm)::Tuple, multi::typeof(closest)) =
    searchsorted_matchix_closest(cond, a, B, perm)


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

searchsorted_matchix(cond::CompositeCondition, a, B, perm) =
    foldl(cond.conds; init=perm) do P, c
        searchsorted_matchix(c, a, B, P)
    end
