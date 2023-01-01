struct ByKey{TFs} <: JoinCondition
    keyfuncs::TFs
end

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
    dct = Dict{
        typeof(keyfunc(first(X))),
        Vector{eltype(keys(X))}
    }()
    for (i, x) in pairs(X)
        vec = get!(dct, keyfunc(x), fill(i, 1))
        last(vec) == i || push!(vec, i)
    end
    evec = valtype(dct)()
    return (dct, evec)
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
    evec = Vector{valtype(dct)}()
    return (dct, evec)
end

findmatchix(::Mode.Hash, cond::ByKey, a, (B, evec)::Tuple, multi::typeof(identity)) = get(B, get_actual_keyfunc(first(cond.keyfuncs))(a), evec)
# two methods with the same body, for resolver disambiguation
findmatchix(::Mode.Hash, cond::ByKey, a, (B, evec)::Tuple, multi::typeof(first)) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    b = get(B, k, nothing)
    isnothing(b) ? evec : [b]
end
findmatchix(::Mode.Hash, cond::ByKey, a, (B, evec)::Tuple, multi::typeof(last)) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    b = get(B, k, nothing)
    isnothing(b) ? evec : [b]
end
