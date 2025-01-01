struct ByPred{TP, TL, TR} <: JoinCondition
    Lf::TL
    Rf::TR
    pred::TP
end

Base.show(io::IO, c::ByPred) = print(io, "by_pred(", c.Lf, ' ', c.pred, ' ', c.Rf, ")")


const _EQUAL_F = Union{typeof(==), typeof(isequal)}

swap_sides(c::ByPred) = ByPred(c.Rf, c.Lf, swap_sides(c.pred))
swap_sides(::typeof(∈)) = ∋
swap_sides(::typeof(∋)) = ∈
swap_sides(::typeof(<)) = >
swap_sides(::typeof(<=)) = >=
swap_sides(f::_EQUAL_F) = f
swap_sides(::typeof(>=)) = <=
swap_sides(::typeof(>)) = <
swap_sides(::typeof(⊆)) = ⊇
swap_sides(::typeof(⊊)) = ⊋
swap_sides(::typeof(⊋)) = ⊊
swap_sides(::typeof(⊇)) = ⊆
swap_sides(f::ComposedFunction{typeof(!), typeof(isdisjoint)}) = f
swap_sides(f::Function) = (a, b) -> f(b, a)

"""
    by_pred(f_L, pred, f_R)

Join condition with `left`-`right` matches defined by `pred(f_L(left), f_R(left))`.

# Examples

```
by_pred(:start_time, <, :time)
by_pred(:time, ∈, :time_range)
```
"""
by_pred(Lf, pred, Rf) = ByPred(normalize_keyfunc(Lf), normalize_keyfunc(Rf), pred)
by_pred(Lf, pred::typeof(≈), Rf; atol) = by_pred(Lf, ∈, Base.Fix2(±, atol) ∘ Rf)


# always supports nested loop
supports_mode(::Mode.NestedLoop, ::ByPred, datas) = true
is_match(by::ByPred, a, b) = by.pred(by.Lf(a), by.Rf(b))
findmatchix(mode::Mode.NestedLoop, cond::ByPred{<:Union{typeof.((<, <=, >=, >))...}}, ix_a, a, B, multi::Closest) =
    @p findmatchix(mode, cond, ix_a, a, B, identity) |>
        firstn_by!(by=i -> abs(cond.Lf(a) - cond.Rf(B[i])))

# support Hash for equality and subset
supports_mode(::Mode.Hash, ::ByPred{<:_EQUAL_F}, datas) = true
supports_mode(::Mode.Hash, cond::ByPred{typeof(∋)}, datas) = Base.isiterable(Core.Compiler.return_type(cond.Lf, Tuple{eltype(datas[1])}))

# order predicates: support Sort
supports_mode(::Mode.SortChain, ::ByPred{<:_EQUAL_F}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, >=, >, ∋))...}}, datas) = true

# intervals set-operations: subset support Sort
supports_mode(::Mode.Sort, cond::ByPred{<:Union{typeof.((⊋, ⊇))...}}, datas) =
    Core.Compiler.return_type(cond.Lf, Tuple{eltype(datas[1])}) <: Interval &&  Core.Compiler.return_type(cond.Rf, Tuple{eltype(datas[2])}) <: Interval
# overlap supports Tree
supports_mode(::Mode.Tree, cond::ByPred{typeof((!) ∘ isdisjoint)}, datas) =
    Core.Compiler.return_type(cond.Lf, Tuple{eltype(datas[1])}) <: Interval &&  Core.Compiler.return_type(cond.Rf, Tuple{eltype(datas[2])}) <: Interval


# Hash implementation
prepare_for_join(mode::Mode.Hash, X, cond::ByPred{<:_EQUAL_F}, multi) = prepare_for_join(mode, X, by_key(nothing, cond.Rf), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{<:_EQUAL_F}, ix_a, a, Bdata, multi) = findmatchix(mode, by_key(cond.Lf, nothing), ix_a, a, Bdata, multi)

prepare_for_join(mode::Mode.Hash, X, cond::ByPred{typeof(∋)}, multi) = prepare_for_join(mode, X, by_key(nothing, cond.Rf), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{typeof(∋)}, ix_a, a, Bdata, multi) =
    @p cond.Lf(a) |>
        Iterators.map(findmatchix(mode, by_key(identity, nothing), nothing, _, Bdata, multi)) |>
        Iterators.flatten() |>
        unique |>
        matchix_postprocess_multi(__, multi)


# Sort implementation for comparisons
sort_byf(cond::ByPred{<:Union{typeof.((<, <=, ==, isequal, >=, >))...}}) = cond.Rf

searchsorted_matchix(cond::ByPred{ typeof(<)}     , a, B, perm) = @inbounds @view perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]
searchsorted_matchix(cond::ByPred{typeof(<=)}     , a, B, perm) = @inbounds @view perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]
searchsorted_matchix(cond::ByPred{typeof(isequal)}, a, B, perm) = @inbounds @view perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]
searchsorted_matchix(cond::ByPred{typeof(==)}     , a, B, perm) = @inbounds @view perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]  # should add `lt= <`? is it allowed?
searchsorted_matchix(cond::ByPred{typeof(>=)}     , a, B, perm) = @inbounds @view perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]
searchsorted_matchix(cond::ByPred{ typeof(>)}     , a, B, perm) = @inbounds @view perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(<), typeof(<=)}}, a, B, perm) = @inbounds @view searchsorted_matchix(cond, a, B, perm)[begin:min(begin, end)]
searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(>), typeof(>=)}}, a, B, perm) = @inbounds @view searchsorted_matchix(cond, a, B, perm)[max(begin, end):end]


# intervals:
# Sort for member queries
sort_byf(cond::ByPred{typeof(∋)}) = cond.Rf

function searchsorted_matchix(cond::ByPred{typeof(∋)}, a, B, perm)
    arr = mapview(i -> cond.Rf(@inbounds B[i]), perm)
    # like view(perm, searchsorted_in(arr, cond.Lf(a))), but also works with non-array iterable indices
    mapview(i -> perm[i], searchsorted_in(arr, cond.Lf(a)))
end

# Sort for subset queries
sort_byf(cond::ByPred{<:Union{typeof.((⊋, ⊇))...}}) = leftendpoint ∘ cond.Rf

function searchsorted_matchix(cond::ByPred{<:Union{typeof.((⊇, ⊋))...}}, a, B, perm)
    leftint = cond.Lf(a)
    @inbounds @p begin
        mapview(i -> leftendpoint(cond.Rf(B[i])), perm)
        searchsorted_in(__, leftint)
        @view perm[__]
        filter(cond.pred(leftint, cond.Rf(B[_])))
    end
end

# helper functions
searchsorted_in(A, X) = @p X |> Iterators.map(searchsorted(A, _)) |> Iterators.flatten() |> unique
searchsorted_in(arr, int::Interval) = searchsorted_interval(arr, int)
