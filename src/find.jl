# findmatchix(cond::JoinCondition, a, B, multi::typeof(identity)) = findall(is_match(cond, a), B)
# findmatchix(cond::JoinCondition, a, B, multi::typeof(first)) = let
#     ix = findfirst(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end
# findmatchix(cond::JoinCondition, a, B, multi::typeof(last)) = let
#     ix = findlast(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end

function optimize(X, cond::ByKey, multi::typeof(identity))
    dct = Dict{
        typeof(cond.keyfunc(first(X))),
        Vector{eltype(keys(X))}
    }()
    for (i, x) in pairs(X)
        push!(get!(dct, cond.keyfunc(x), []), i)
    end
    return dct
end

function optimize(X, cond::ByKey, multi::typeof(first))
    dct = Dict{
        typeof(cond.keyfunc(first(X))),
        eltype(keys(X))
    }()
    for (i, x) in pairs(X)
        get!(dct, cond.keyfunc(x), i)
    end
    return dct
end

function optimize(X, cond::ByKey, multi::typeof(last))
    dct = Dict{
        typeof(cond.keyfunc(first(X))),
        eltype(keys(X))
    }()
    for (i, x) in pairs(X)
        dct[cond.keyfunc(x)] = i
    end
    return dct
end

findmatchix(cond::ByKey, a, B::Dict, multi::typeof(identity)) = get(B, cond.keyfunc(a), valtype(B)())
findmatchix(cond::ByKey, a, B::Dict, multi::Union{typeof(first), typeof(last)}) = let
    haskey(B, cond.keyfunc(a)) ? [B[cond.keyfunc(a)]] : Vector{valtype(B)}()
end
