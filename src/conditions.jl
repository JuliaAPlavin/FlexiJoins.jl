struct Closest end
const closest = Closest()


abstract type JoinCondition end


module Mode
struct Hash end
struct SortChain end
struct Sort end
struct Tree end
struct NestedLoop end
struct NestedLoopFast end
end

supports_mode(mode, cond, datas) = false
supports_mode(::Mode.Sort, cond, datas) = supports_mode(Mode.SortChain(), cond, datas)

choose_mode(mode, cond, datas) = supports_mode(mode, cond, datas) ? mode : nothing
choose_mode(mode::Nothing, cond, datas) =
    supports_mode(Mode.Hash(), cond, datas) ? Mode.Hash() :
    supports_mode(Mode.Tree(), cond, datas) ? Mode.Tree() :
    supports_mode(Mode.Sort(), cond, datas) ? Mode.Sort() :
    supports_mode(Mode.NestedLoopFast(), cond, datas) ? Mode.NestedLoopFast() :
    nothing

preferred_first_side(datas, cond, ::Mode.Sort) = StaticInt(length(datas[1]) > length(datas[2]) ? 2 : 1)
preferred_first_side(datas, cond, ::Mode.Hash) = StaticInt(length(datas[1]) < length(datas[2]) ? 2 : 1)
preferred_first_side(datas, cond, ::Mode.Tree) = StaticInt(length(datas[1]) > length(datas[2]) ? 2 : 1)


normalize_keyfunc(x::Tuple) = let
    funcs = map(normalize_keyfunc, x)
    arg -> map(f -> f(arg), funcs)
end
normalize_keyfunc(x::ComposedFunction) = normalize_keyfunc(x.outer) ∘ normalize_keyfunc(x.inner)
normalize_keyfunc(x) = x
normalize_keyfunc(x::Symbol) = Accessors.PropertyLens{x}()


findmatchix(mode, cond, ix_a, a, Bdata, multi) = findmatchix(mode, cond, a, Bdata, multi)
findmatchix(mode::Union{Mode.NestedLoop, Mode.Sort, Mode.Tree}, cond::JoinCondition, ix_a, a, B_prep, multi::Union{typeof(first), typeof(last)}) = matchix_postprocess_multi(findmatchix(mode, cond, ix_a, a, B_prep, identity), multi)


prepare_for_join(::Union{Mode.NestedLoop, Mode.NestedLoopFast}, X, cond::JoinCondition) = X
findmatchix(::Union{Mode.NestedLoop, Mode.NestedLoopFast}, cond::JoinCondition, ix_a, a, B, multi::typeof(identity)) =
    @p pairs(B) |>
        Iterators.filter(is_match(cond, ix_a, a, _[1], _[2])) |>
        map(_[1])
is_match(cond, ix_a, a, ix_b, b) = is_match(cond, a, b)


matchix_postprocess_multi(IX, ::typeof(identity)) = IX
matchix_postprocess_multi(IX, ::typeof(first)) = propagate_empty(minimum, IX)
matchix_postprocess_multi(IX, ::typeof(last)) = propagate_empty(maximum, IX)
propagate_empty(func::typeof(identity), arr) = arr
propagate_empty(func::Union{typeof.((first, last))...}, arr) = func(arr, 1)
propagate_empty(func::Union{typeof.((minimum, maximum))...}, arr) = isempty(arr) ? MaybeVector{_eltype(arr)}() : MaybeVector{_eltype(arr)}(func(arr))


prepare_for_join(::Mode.Sort, X, cond::JoinCondition) = (X, sortperm(X; by=sort_byf(cond)))

findmatchix(::Mode.Sort, cond::JoinCondition, ix_a, a, (B, perm)::Tuple, multi::typeof(identity)) = searchsorted_matchix(cond, a, B, perm)
findmatchix(::Mode.Sort, cond::JoinCondition, ix_a, a, (B, perm)::Tuple, multi::typeof(closest)) = searchsorted_matchix_closest(cond, a, B, perm)


struct CompositeCondition{TC} <: JoinCondition
    conds::TC
end

Base.show(io::IO, c::CompositeCondition) = print(io, join(c.conds, " & "))

is_match(by::CompositeCondition, args...) = all(by1 -> is_match(by1, args...), by.conds)

Base.:(&)(a::JoinCondition, b::JoinCondition) = CompositeCondition((a, b))
Base.:(&)(a::CompositeCondition, b::JoinCondition) = CompositeCondition((a.conds..., b))
Base.:(&)(a::JoinCondition, b::CompositeCondition) = CompositeCondition((a, b.conds))
Base.:(&)(a::CompositeCondition, b::CompositeCondition) = CompositeCondition((a.conds..., b.conds...))

swap_sides(c::CompositeCondition) = CompositeCondition(map(swap_sides, c.conds))

normalize_arg(cond::CompositeCondition, datas) = CompositeCondition(map(c -> normalize_arg(c, datas), cond.conds))

supports_mode(mode::Mode.NestedLoop, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.SortChain, cond::CompositeCondition, datas) = all(c -> supports_mode(mode, c, datas), cond.conds)
supports_mode(mode::Mode.Hash, cond::CompositeCondition, datas) = length(cond.conds) == 2 && supports_mode(mode, first(cond.conds), datas) && all(c -> supports_mode(Mode.NestedLoopFast(), c, datas), Base.tail(cond.conds))
supports_mode(mode::Mode.Sort, cond::CompositeCondition, datas) =
    all(c -> supports_mode(Mode.SortChain(), c, datas), cond.conds[1:end-1]) && supports_mode(mode, last(cond.conds), datas)


prepare_for_join(mode::Mode.Hash, X, cond::CompositeCondition, multi) = prepare_for_join(mode, X, first(cond.conds), multi)

findmatchix(mode::Mode.Hash, cond::CompositeCondition, ix_a, a, X, multi) = 
    @p findmatchix(mode, first(cond.conds), ix_a, a, X, multi) |>
        Iterators.filter(is_match_ix(last(cond.conds), ix_a, _))


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
