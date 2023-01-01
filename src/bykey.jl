struct ByKey{TFs} <: JoinCondition
    keyfuncs::TFs
end

supports_mode(::Mode.NestedLoop, ::ByKey, datas) = true
supports_mode(::Mode.SortChain, ::ByKey, datas) = true
supports_mode(::Mode.Hash, ::ByKey, datas) = true

# function index end
# by_key(keyfunc::typeof(index)) = ByKey(only ∘ parentindices)
by_key(keyfunc) = ByKey(keyfunc)

# is_match(by::ByKey, a, b) = by.keyfunc(a) == by.keyfunc(b)

# extra(::Val{:key}, by::ByKey, a::Nothing, b) = by.keyfunc(b)
# extra(::Val{:key}, by::ByKey, a, b::Nothing) = by.keyfunc(a)
# extra(::Val{:key}, by::ByKey, a, b) = (@assert by.keyfunc(a) == by.keyfunc(b); by.keyfunc(a))

normalize_arg(cond::ByKey{<:NamedTuple{NSk}}, datas::NamedTuple{NS}) where {NSk, NS} = (@assert NSk == NS; ByKey(map(normalize_keyfunc, cond.keyfuncs) |> values))
normalize_arg(cond::ByKey, datas::Union{Tuple, NamedTuple}) = ByKey(map(Returns(normalize_keyfunc(cond.keyfuncs)), datas) |> values)
normalize_keyfunc(x::Tuple) = map(x -> only(normalize_keyfunc(x)), x)
normalize_keyfunc(x) = (x,)
normalize_keyfunc(x::Symbol) = (Accessors.PropertyLens{x}(),)
get_actual_keyfunc(x::Tuple) = arg -> map(el -> el(arg), x)


function optimize(::Mode.Hash, which, datas, cond::ByKey, multi::typeof(identity))
    keyfunc = get_actual_keyfunc(which(cond.keyfuncs))
    X = which(datas)
    dct = Dict{
        typeof(keyfunc(first(X))),
        Vector{eltype(keys(X))}
    }()
    for (i, x) in pairs(X)
        push!(get!(dct, keyfunc(x), []), i)
    end
    return dct
end

function optimize(::Mode.Hash, which, datas, cond::ByKey, multi::Union{typeof(first), typeof(last)})
    keyfunc = get_actual_keyfunc(which(cond.keyfuncs))
    X = which(datas)
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

findmatchix(::Mode.Hash, cond::ByKey, a, B::Dict, multi::typeof(identity)) = get(B, get_actual_keyfunc(first(cond.keyfuncs))(a), valtype(B)())
findmatchix(::Mode.Hash, cond::ByKey, a, B::Dict, multi::Union{typeof(first), typeof(last)}) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    haskey(B, k) ? [B[k]] : Vector{valtype(B)}()
end
