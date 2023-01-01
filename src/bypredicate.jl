struct ByPred{TP, TL, TR} <: JoinCondition
    Lf::TL
    Rf::TR
    pred::TP
end

swap_sides(c::ByPred) = ByPred(c.Rf, c.Lf, swap_sides(c.pred))
swap_sides(::typeof(∈)) = ∋
swap_sides(::typeof(∋)) = ∈
swap_sides(::typeof(<)) = >
swap_sides(::typeof(<=)) = >=
swap_sides(::typeof(==)) = ==
swap_sides(::typeof(>=)) = <=
swap_sides(::typeof(>)) = <
swap_sides(::typeof(⊆)) = ⊇
swap_sides(::typeof(⊊)) = ⊋
swap_sides(::typeof(⊋)) = ⊊
swap_sides(::typeof(⊇)) = ⊆
swap_sides(f::ComposedFunction{typeof(!), typeof(isdisjoint)}) = f

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


# always supports nested loop
supports_mode(::Mode.NestedLoop, ::ByPred, datas) = true
is_match(by::ByPred, a, b) = by.pred(by.Lf(a), by.Rf(b))
findmatchix(mode::Mode.NestedLoop, cond::ByPred{<:Union{typeof.((<, <=, >=, >))...}}, ix_a, a, B, multi::Closest) =
    @p findmatchix(mode, cond, ix_a, a, B, identity) |>
        firstn_by!(by=i -> abs(cond.Lf(a) - cond.Rf(B[i])))

# support Hash for equality and subset
supports_mode(::Mode.Hash, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Hash, cond::ByPred{typeof(∋)}, datas) = Base.isiterable(Core.Compiler.return_type(cond.Lf, Tuple{valtype(datas[1])}))

# order predicates: support Sort
supports_mode(::Mode.SortChain, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, datas) = true

# intervals set-operations: subset support Sort
supports_mode(::Mode.Sort, cond::ByPred{<:Union{typeof.((⊋, ⊇))...}}, datas) =
    Core.Compiler.return_type(cond.Lf, Tuple{valtype(datas[1])}) <: Interval &&  Core.Compiler.return_type(cond.Rf, Tuple{valtype(datas[2])}) <: Interval
# overlap supports Tree
supports_mode(::Mode.Tree, cond::ByPred{typeof((!) ∘ isdisjoint)}, datas) =
    Core.Compiler.return_type(cond.Lf, Tuple{valtype(datas[1])}) <: Interval &&  Core.Compiler.return_type(cond.Rf, Tuple{valtype(datas[2])}) <: Interval


# Hash implementation
prepare_for_join(mode::Mode.Hash, X, cond::ByPred{typeof(==)}, multi) = prepare_for_join(mode, X, by_key(nothing, cond.Rf), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{typeof(==)}, ix_a, a, Bdata, multi) = findmatchix(mode, by_key(cond.Lf, nothing), ix_a, a, Bdata, multi)

prepare_for_join(mode::Mode.Hash, X, cond::ByPred{typeof(∋)}, multi) = prepare_for_join(mode, X, by_key(nothing, cond.Rf), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{typeof(∋)}, ix_a, a, Bdata, multi) =
    @p cond.Lf(a) |>
        Iterators.map(findmatchix(mode, by_key(identity, nothing), nothing, _, Bdata, multi)) |>
        Iterators.flatten() |>
        unique |>
        matchix_postprocess_multi(__, multi)


# Sort implementation for comparisons
sort_byf(cond::ByPred{<:Union{typeof.((<, <=, ==, >=, >))...}}) = cond.Rf

@inbounds searchsorted_matchix(cond::ByPred{ typeof(<)}, a, B, perm) = @view perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]
@inbounds searchsorted_matchix(cond::ByPred{typeof(<=)}, a, B, perm) = @view perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]
@inbounds searchsorted_matchix(cond::ByPred{typeof(==)}, a, B, perm) = @view perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]
@inbounds searchsorted_matchix(cond::ByPred{typeof(>=)}, a, B, perm) = @view perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]
@inbounds searchsorted_matchix(cond::ByPred{ typeof(>)}, a, B, perm) = @view perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

@inbounds searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(<), typeof(<=)}}, a, B, perm) = @view searchsorted_matchix(cond, a, B, perm)[begin:min(begin, end)]
@inbounds searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(>), typeof(>=)}}, a, B, perm) = @view searchsorted_matchix(cond, a, B, perm)[max(begin, end):end]


# intervals:
# Sort for member queries
sort_byf(cond::ByPred{typeof(∋)}) = cond.Rf

@inbounds function searchsorted_matchix(cond::ByPred{typeof(∋)}, a, B, perm)
    arr = mapview(i -> cond.Rf(B[i]), perm)
    _do_view(perm, searchsorted_in(arr, cond.Lf(a)))
end

# Sort for subset queries
sort_byf(cond::ByPred{<:Union{typeof.((⊋, ⊇))...}}) = leftendpoint ∘ cond.Rf

@inbounds function searchsorted_matchix(cond::ByPred{<:Union{typeof.((⊇, ⊋))...}}, a, B, perm)
    leftint = cond.Lf(a)
    @p begin
        mapview(i -> leftendpoint(cond.Rf(B[i])), perm)
        searchsorted_in(__, leftint)
        @view perm[__]
        filter(cond.pred(leftint, cond.Rf(B[_])))
    end
end

# Tree for overlaps
prepare_for_join(::Mode.Tree, X, cond::ByPred{typeof((!) ∘ isdisjoint)}) =
    (X, NN.KDTree(map(as_vector ∘ endpoints ∘ cond.Rf, X) |> wrap_matrix, NN.Euclidean()))
function findmatchix(::Mode.Tree, cond::ByPred{typeof((!) ∘ isdisjoint)}, ix_a, a, (B, tree)::Tuple, multi::typeof(identity))
    leftint = cond.Lf(a)
    @p inrect(tree, as_vector((-Inf, leftendpoint(leftint))), as_vector((rightendpoint(leftint), Inf))) |>
        filter!(cond.pred(leftint, cond.Rf(B[_])))
end


# helper functions
searchsorted_in(A, X) = @p X |> Iterators.map(searchsorted(A, _)) |> Iterators.flatten() |> unique

if isdefined(IntervalSets, :searchsorted_interval)
    # IntervalSets 0.7.1+
    # at some point, remove the block below and bump compat
    searchsorted_in(arr, int::Interval) = searchsorted_interval(arr, int)
else
    searchsorted_in(arr, int::Interval{:closed, :closed}) = searchsortedfirst(arr, minimum(int)):searchsortedlast(arr, maximum(int))
    searchsorted_in(arr, int::Interval{:closed,   :open}) = searchsortedfirst(arr, minimum(int)):(searchsortedfirst(arr, supremum(int)) - 1)
    searchsorted_in(arr, int::Interval{  :open, :closed}) = (searchsortedlast(arr, infimum(int)) + 1):searchsortedlast(arr, maximum(int))
    searchsorted_in(arr, int::Interval{  :open,   :open}) = (searchsortedlast(arr, infimum(int)) + 1):(searchsortedfirst(arr, supremum(int)) - 1)
end


Base.show(io::IO, c::ByPred) = print(io, "by_pred(", c.Lf, ' ', c.pred, ' ', c.Rf, ")")
