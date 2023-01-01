# findmatchix(cond::JoinCondition, a, B, multi::typeof(identity)) = findall(is_match(cond, a), B)
# findmatchix(cond::JoinCondition, a, B, multi::typeof(first)) = let
#     ix = findfirst(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end
# findmatchix(cond::JoinCondition, a, B, multi::typeof(last)) = let
#     ix = findlast(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end

function optimize(which, datas, cond::ByKey, multi::typeof(identity))
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

function optimize(which, datas, cond::ByKey, multi::Union{typeof(first), typeof(last)})
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

findmatchix(cond::ByKey, a, B::Dict, multi::typeof(identity)) = get(B, get_actual_keyfunc(first(cond.keyfuncs))(a), valtype(B)())
findmatchix(cond::ByKey, a, B::Dict, multi::Union{typeof(first), typeof(last)}) = let
    k = get_actual_keyfunc(first(cond.keyfuncs))(a)
    haskey(B, k) ? [B[k]] : Vector{valtype(B)}()
end
