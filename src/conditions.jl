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

choose_mode(mode, cond, datas) = supports_mode(mode, cond, datas) ? mode : nothing
choose_mode(mode::Nothing, cond, datas) =
    supports_mode(Mode.Hash(), cond, datas) ? Mode.Hash() :
    supports_mode(Mode.Tree(), cond, datas) ? Mode.Tree() :
    supports_mode(Mode.Sort(), cond, datas) ? Mode.Sort() :
    nothing

preferred_first_side(datas, cond, ::Mode.Sort) = length(datas[1]) > length(datas[2]) ? StaticInt(2) : StaticInt(1)
preferred_first_side(datas, cond, ::Mode.Hash) = length(datas[1]) < length(datas[2]) ? StaticInt(2) : StaticInt(1)
preferred_first_side(datas, cond, ::Mode.Tree) = length(datas[1]) > length(datas[2]) ? StaticInt(2) : StaticInt(1)


normalize_keyfunc(x::Tuple) = let
    funcs = map(normalize_keyfunc, x)
    arg -> map(f -> f(arg), funcs)
end
normalize_keyfunc(x::ComposedFunction) = @modify(normalize_keyfunc, x |> Properties())
normalize_keyfunc(x) = x
normalize_keyfunc(x::Symbol) = Accessors.PropertyLens{x}()


findmatchix(mode, cond::JoinCondition, a, B_prep, multi::typeof(first)) = propagate_empty(minimum, findmatchix(mode, cond, a, B_prep, identity))
findmatchix(mode, cond::JoinCondition, a, B_prep, multi::typeof(last)) = propagate_empty(maximum, findmatchix(mode, cond, a, B_prep, identity))


prepare_for_join(::Mode.NestedLoop, X, cond::JoinCondition) = X
function findmatchix(::Mode.NestedLoop, cond::JoinCondition, a, B, multi::typeof(identity))
    res = keytype(B)[]
    for (i, b) in pairs(B)
        if is_match(cond, a, b)
            push!(res, i)
        end
    end
    return res
end
firstn_by!(A::Vector, n=1; by) = view(partialsort!(A, 1:min(n, length(A)); by), 1:min(n, length(A)))

propagate_empty(func::typeof(identity), arr) = arr
propagate_empty(func::Union{typeof.((first, last))...}, arr) = func(arr, 1)
propagate_empty(func::Union{typeof.((minimum, maximum))...}, arr) = isempty(arr) ? arr : [func(arr)]


prepare_for_join(::Mode.Sort, X, cond::JoinCondition) = (X, sortperm(X; by=sort_byf(cond)))

findmatchix(::Mode.Sort, cond::JoinCondition, a, (B, perm)::Tuple, multi::typeof(identity)) =
    searchsorted_matchix(cond, a, B, perm)  # sort to keep same order?
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

swap_sides(c::CompositeCondition) = CompositeCondition(map(swap_sides, c.conds))

normalize_arg(cond::CompositeCondition, datas) = CompositeCondition(map(c -> normalize_arg(c, datas), cond.conds))

supports_mode(mode::Mode.NestedLoop, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.SortChain, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.Sort, cond::CompositeCondition, datas) =
    all(c -> supports_mode(Mode.SortChain(), c, datas), cond.conds[1:end-1]) && supports_mode(mode, last(cond.conds), datas)

sort_byf(cond::CompositeCondition) = x -> map(c -> sort_byf(c)(x), cond.conds)

searchsorted_matchix(cond::CompositeCondition, a, B, perm) =
    foldl(cond.conds; init=perm) do P, c
        searchsorted_matchix(c, a, B, P)
    end

searchsorted_matchix_closest(cond::CompositeCondition, a, B, perm) =
    @p  foldl(Base.front(cond.conds); init=perm) do P, c
            searchsorted_matchix(c, a, B, P)
        end |>
        searchsorted_matchix_closest(last(cond.conds), a, B, __)


Base.show(io::IO, c::CompositeCondition) = print(io, join(c.conds, " & "))
