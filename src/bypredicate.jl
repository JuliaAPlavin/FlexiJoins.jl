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

innerfunc(f::ComposedFunction) = innerfunc(f.inner)
innerfunc(f) = f
stripinner(f::ComposedFunction) = f.inner isa ComposedFunction ? f.outer ∘ stripinner(f.inner) : f.outer

normalize_arg(cond::ByPred, datas) = (@assert length(datas) == 2; cond)


supports_mode(::Mode.NestedLoop, ::ByPred, datas) = true
is_match(by::ByPred, a, b) = by.pred(by.Lf(a), by.Rf(b))
findmatchix(::Mode.NestedLoop, cond::ByPred{<:Union{typeof.((<, <=, >=, >))...}}, a, B, multi::Closest) =
    @p B |>
        findall(b -> is_match(cond, a, b)) |>
        sort(by=i -> abs(cond.Lf(a) - cond.Rf(B[i]))) |>
        first(__, 1)


supports_mode(::Mode.SortChain, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, datas) = true

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
    int = cond.Lf(a)
    @assert int isa Interval
    arr = mapview(i -> cond.Rf(B[i]), perm)
    @view perm[searchsorted_interval(arr, int)]
end

searchsorted_interval(arr, int::Interval{:closed, :closed}) = searchsortedfirst(arr, minimum(int)):searchsortedlast(arr, maximum(int))
searchsorted_interval(arr, int::Interval{:closed,   :open}) = searchsortedfirst(arr, minimum(int)):(searchsortedfirst(arr, supremum(int)) - 1)
searchsorted_interval(arr, int::Interval{  :open, :closed}) = (searchsortedlast(arr, infimum(int)) + 1):searchsortedlast(arr, maximum(int))
searchsorted_interval(arr, int::Interval{  :open,   :open}) = (searchsortedlast(arr, infimum(int)) + 1):(searchsortedfirst(arr, supremum(int)) - 1)


Base.show(io::IO, c::ByPred) = print(io, "by_pred(", c.Lf, ' ', c.pred, ' ', c.Rf, ")")
