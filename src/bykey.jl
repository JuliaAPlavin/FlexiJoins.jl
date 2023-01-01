struct ByIndex end
(::ByIndex)(_) = error("ByIndex is not callable and requires a specialized join implementation")
by_index = ByIndex()
export by_index


struct ByKey{TFs} <: JoinCondition
    keyfuncs::TFs
end

swap_sides(c::ByKey) = ByKey(swap_sides(c.keyfuncs))

eval_keyfunc(f, ix, x) = f(x)
eval_keyfunc(f::ByIndex, ix, x) = ix
keyval(c::ByKey, f, ix, x) = eval_keyfunc(f(c.keyfuncs), ix, x)
keyval(c::ByKey, f, x) = keyval(c::ByKey, f, nothing, x)


"""
    by_key(f)
    by_key(f_L, f_R)

Join condition with `left`-`right` matches defined by `f_L(left) == f_R(right)`.

# Examples

```
by_key(:name)
by_key(:name, x -> first(x.names))
```
"""
by_key(keyfunc) = ByKey((normalize_keyfunc(keyfunc),))
by_key(f_L, f_R) = ByKey(normalize_keyfunc.((f_L, f_R)))
by_key(; keyfuncs...) = ByKey(map(normalize_keyfunc, values(keyfuncs)))

normalize_arg(cond::ByKey{<:NamedTuple{NSk}}, datas::NamedTuple{NS}) where {NSk, NS} = (@assert NSk == NS; ByKey(cond.keyfuncs |> values))
normalize_arg(cond::ByKey{<:Tuple{Any}}, datas::Union{Tuple, NamedTuple}) = ByKey(ntuple(Returns(only(cond.keyfuncs)), length(datas)))
normalize_arg(cond::ByKey{<:Tuple}, datas::Union{Tuple, NamedTuple}) = (@assert length(cond.keyfuncs) == length(datas); ByKey(cond.keyfuncs))


# nested loop implementation
supports_mode(::Mode.NestedLoop, ::ByKey, datas) = true
is_match(by::ByKey, a, b) = keyval(by, first, a) == keyval(by, last, b)


# Sort implementation
supports_mode(::Mode.SortChain, ::ByKey, datas) = true
sort_byf(cond::ByKey) = last(cond.keyfuncs)
@inbounds searchsorted_matchix(cond::ByKey, a, B, perm) =
    @view perm[searchsorted(
        mapview(i -> keyval(cond, last, i, B[i]), perm),
        keyval(cond, first, a)
    )]


# Hash implementation
supports_mode(::Mode.Hash, ::ByKey, datas) = true

# return all matches, multi=identity
function prepare_for_join(::Mode.Hash, X, cond::ByKey, multi::typeof(identity))
    keyfunc = last(cond.keyfuncs)

    ngroups = 0
    groups = similar(X, Int)
    dct = Dict{Core.Compiler.return_type(keyfunc, Tuple{valtype(X)}), Int}()
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

@inbounds function findmatchix(::Mode.Hash, cond::ByKey, ix_a, a, (dct, starts, rperm)::Tuple, multi::typeof(identity))
    group_id = get(dct, keyval(cond, first, ix_a, a), -1)
    group_id == -1 ?
        @view(rperm[1:1:0]) :
        @view(rperm[starts[group_id + 1]:-1:1 + starts[group_id]])
end

# first/last match only
function prepare_for_join(::Mode.Hash, X, cond::ByKey, multi::Union{typeof(first), typeof(last)})
    keyfunc = last(cond.keyfuncs)
    dct = Dict{
        Core.Compiler.return_type(keyfunc, Tuple{valtype(X)}),
        keytype(X)
    }()
    for (i, x) in pairs(X)
        multi === first && get!(dct, keyfunc(x), i)
        multi === last && (dct[keyfunc(x)] = i)
    end
    return dct
end

findmatchix(::Mode.Hash, cond::ByKey, ix_a, a, B, multi::Union{typeof(first), typeof(last)}) = let
    b = get(B, keyval(cond, first, ix_a, a), nothing)
    isnothing(b) ? MaybeVector{valtype(B)}() : MaybeVector{valtype(B)}(b)
end


# by index
# ByIndex on the left: handled by the generic implementation above
# ByIndex on the right: same implementation, different methods for disambiguation
for F in (identity, first, last)
    @eval prepare_for_join(::Mode.Hash, X, cond::ByKey{<:Tuple{<:Any, ByIndex}}, multi::typeof($F)) = keys(X)

    @eval @inbounds function findmatchix(::Mode.Hash, cond::ByKey{<:Tuple{<:Any, ByIndex}}, ix_a, a, B, multi::typeof($F))
        b = get(B, keyval(cond, first, ix_a, a), nothing)
        isnothing(b) ? MaybeVector{valtype(B)}() : MaybeVector{valtype(B)}(b)
    end
end



function Base.show(io::IO, c::ByKey)
    print(io, "by_key(")
    for (i, f) in enumerate(c.keyfuncs)
        i > 1 && print(io, ", ")
        show(io, f)
    end
    print(io, ")")
end
