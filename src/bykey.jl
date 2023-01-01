struct ByKey{TFs} <: JoinCondition
    keyfuncs::TFs
end

swap_sides(c::ByKey) = ByKey(swap_sides(c.keyfuncs))

"""
    by_key(f)
    by_key((f_L, f_R))

Join condition with `left`-`right` matches defined by `f_L(left) == f_R(right)`.

# Examples

```
by_key(:name)
by_key(:name, x -> first(x.names))
```
"""
by_key(keyfunc) = ByKey(keyfunc)

normalize_arg(cond::ByKey{<:NamedTuple{NSk}}, datas::NamedTuple{NS}) where {NSk, NS} = (@assert NSk == NS; ByKey(map(normalize_keyfunc, cond.keyfuncs) |> values))
normalize_arg(cond::ByKey, datas::Union{Tuple, NamedTuple}) = ByKey(map(Returns(normalize_keyfunc(cond.keyfuncs)), datas) |> values)
normalize_keyfunc(x::Tuple) = map(x -> only(normalize_keyfunc(x)), x)
normalize_keyfunc(x) = (x,)
normalize_keyfunc(x::Symbol) = (Accessors.PropertyLens{x}(),)
get_actual_keyfunc(x::Tuple) = arg -> map(el -> el(arg), x)


supports_mode(::Mode.NestedLoop, ::ByKey, datas) = true
is_match(by::ByKey, a, b) = get_actual_keyfunc(first(by.keyfuncs))(a) == get_actual_keyfunc(last(by.keyfuncs))(b)


supports_mode(::Mode.SortChain, ::ByKey, datas) = true
sort_byf(cond::ByKey) = get_actual_keyfunc(last(cond.keyfuncs))
@inbounds searchsorted_matchix(cond::ByKey, a, B, perm) =
    @view perm[searchsorted(
        mapview(i -> get_actual_keyfunc(last(cond.keyfuncs))(B[i]), perm),
        get_actual_keyfunc(first(cond.keyfuncs))(a)
    )]


supports_mode(::Mode.Hash, ::ByKey, datas) = true

function prepare_for_join(::Mode.Hash, X, cond::ByKey, multi::typeof(identity))
    keyfunc = get_actual_keyfunc(last(cond.keyfuncs))

    ngroups = 0
    groups = similar(X, Int)
    dct = Dict{typeof(keyfunc(first(X))), Int}()
    @inbounds for (i, x) in pairs(X)
        group_id = get!(dct, keyfunc(x), ngroups + 1)
        if group_id == ngroups + 1
            ngroups += 1
        end
        groups[i] = group_id
    end

    starts = zeros(Int, ngroups)
    @inbounds for gix in groups
        starts[gix] += 1
    end
    cumsum!(starts, starts)
    push!(starts, length(groups))

    rperm = Vector{keytype(X)}(undef, length(X))
    @inbounds for (i, gix) in pairs(groups)
        rperm[starts[gix]] = i
        starts[gix] -= 1
    end

    return (dct, starts, rperm)
end

function prepare_for_join(::Mode.Hash, X, cond::ByKey, multi::Union{typeof(first), typeof(last)})
    keyfunc = get_actual_keyfunc(last(cond.keyfuncs))
    dct = Dict{
        typeof(keyfunc(first(X))),
        eltype(keys(X))
    }()
    for (i, x) in pairs(X)
        multi === first && get!(dct, keyfunc(x), i)
        multi === last && (dct[keyfunc(x)] = i)
    end
    return dct
end

@inbounds function findmatchix(::Mode.Hash, cond::ByKey, a, (dct, starts, rperm)::Tuple, multi::typeof(identity))
    group_id = get(dct, get_actual_keyfunc(first(cond.keyfuncs))(a), -1)
    group_id == -1 ?
        @view(rperm[1:1:0]) :
        @view(rperm[starts[group_id + 1]:-1:1 + starts[group_id]])
end
# two methods with the same body, for resolver disambiguation
findmatchix(::Mode.Hash, cond::ByKey, a, B, multi::typeof(first)) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    b = get(B, k, nothing)
    T = _valtype(B)
    isnothing(b) ? MaybeVector{T}() : MaybeVector{T}(b)
end
findmatchix(::Mode.Hash, cond::ByKey, a, B, multi::typeof(last)) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    b = get(B, k, nothing)
    T = _valtype(B)
    isnothing(b) ? MaybeVector{T}() : MaybeVector{T}(b)
end


Base.show(io::IO, c::ByKey) = print(io, "by_key(", c.keyfuncs, ")")
