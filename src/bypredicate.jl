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

"""
    by_pred(f_L, pred, f_R)

Join condition with `left`-`right` matches defined by `pred(f_L(left), f_R(left))`.

# Examples

```
by_pred(:start_time, <, :time)
by_pred(:time, ∈, :time_range)
```
"""
by_pred(Lf, pred, Rf) = ByPred(Lf, Rf, pred)

normalize_arg(cond::ByPred, datas) = (@assert length(datas) == 2; cond)


supports_mode(::Mode.NestedLoop, ::ByPred, datas) = true
is_match(by::ByPred, a, b) = by.pred(by.Lf(a), by.Rf(b))
findmatchix(mode::Mode.NestedLoop, cond::ByPred{<:Union{typeof.((<, <=, >=, >))...}}, a, B, multi::Closest) =
    @p findmatchix(mode, cond, a, B, identity) |>
        firstn_by!(by=i -> abs(cond.Lf(a) - cond.Rf(B[i])))

supports_mode(::Mode.Hash, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Hash, cond::ByPred{typeof(∋)}, datas) = Base.isiterable(Core.Compiler.return_type(cond.Lf, Tuple{eltype(datas[1])}))
supports_mode(::Mode.SortChain, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, datas) = true


prepare_for_join(mode::Mode.Hash, X, cond::ByPred{typeof(==)}, multi) = prepare_for_join(mode, X, ByKey((nothing, (cond.Rf,))), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{typeof(==)}, a, Bdata, multi) = findmatchix(mode, ByKey(((cond.Lf,), nothing)), a, Bdata, multi)

prepare_for_join(mode::Mode.Hash, X, cond::ByPred{typeof(∋)}, multi) = prepare_for_join(mode, X, ByKey((nothing, (cond.Rf,))), multi)
findmatchix(mode::Mode.Hash, cond::ByPred{typeof(∋)}, a, Bdata, multi::typeof(identity)) =
    mapreduce(vcat, cond.Lf(a)) do aa
        findmatchix(mode, ByKey(((identity,), nothing)), aa, Bdata, multi)
    end


sort_byf(cond::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}) = cond.Rf

@inbounds searchsorted_matchix(cond::ByPred{typeof(<)}, a, B, perm) =
    @view perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]

@inbounds searchsorted_matchix(cond::ByPred{typeof(<=)}, a, B, perm) =
    @view perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]

@inbounds searchsorted_matchix(cond::ByPred{typeof(==)}, a, B, perm) =
    @view perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

@inbounds searchsorted_matchix(cond::ByPred{typeof(>=)}, a, B, perm) =
    @view perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

@inbounds searchsorted_matchix(cond::ByPred{typeof(>)}, a, B, perm) =
    @view perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

@inbounds searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(<), typeof(<=)}}, a, B, perm) =
    @view searchsorted_matchix(cond, a, B, perm)[begin:min(begin, end)]
@inbounds searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(>), typeof(>=)}}, a, B, perm) =
    @view searchsorted_matchix(cond, a, B, perm)[max(begin, end):end]

@inbounds function searchsorted_matchix(cond::ByPred{typeof(∋)}, a, B, perm)
    arr = mapview(i -> cond.Rf(B[i]), perm)
    @view perm[searchsorted_in(arr, cond.Lf(a))]
end

searchsorted_in(A, X) = mapreduce(x -> searchsorted(A, x), vcat, X)

searchsorted_in(arr, int::Interval{:closed, :closed}) = searchsortedfirst(arr, minimum(int)):searchsortedlast(arr, maximum(int))
searchsorted_in(arr, int::Interval{:closed,   :open}) = searchsortedfirst(arr, minimum(int)):(searchsortedfirst(arr, supremum(int)) - 1)
searchsorted_in(arr, int::Interval{  :open, :closed}) = (searchsortedlast(arr, infimum(int)) + 1):searchsortedlast(arr, maximum(int))
searchsorted_in(arr, int::Interval{  :open,   :open}) = (searchsortedlast(arr, infimum(int)) + 1):(searchsortedfirst(arr, supremum(int)) - 1)


Base.show(io::IO, c::ByPred) = print(io, "by_pred(", c.Lf, ' ', c.pred, ' ', c.Rf, ")")
