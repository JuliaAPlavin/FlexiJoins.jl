module FlexiJoins

using StructArrays
using Static: StaticInt, known
using Accessors
using DataPipes
using Indexing


export
    flexijoin, joinindices, @optic,
    by_key, by_distance,
    keep, drop


include("nothingindex.jl")
include("conditions.jl")
include("normalize_specs.jl")
include("ix_compute.jl")


function flexijoin(datas, cond; kwargs...)
	IXs = joinindices(datas, cond; kwargs...)
    myview(datas, IXs)
end

function joinindices(datas::NamedTuple{NS}, cond; kwargs...) where {NS}
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(NamedTuple{NS}(StructArrays.components(IXs_unnamed)))
end

function joinindices(datas::Tuple, cond; kwargs...)
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(StructArrays.components(IXs_unnamed))
end

function _joinindices(datas, cond; kwargs...)
    _joinindices(
        values(datas),
        stripnames(cond, datas),
        stripnames(get(kwargs, :multi, nothing), datas; default=identity),
        stripnames(get(kwargs, :nonmatches, nothing), datas; default=drop),
        stripname_groupby(get(kwargs, :groupby, nothing), datas),
        stripnames(get(kwargs, :cardinality, nothing), datas; default=*),
    )
end

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality)
    if any(@. multi !== identity && nonmatches !== drop)
        error("Values of arguments don't make sense together: ", (; nonmatches, multi))
    end
	IXs = create_ix_array(datas, nonmatches, groupby)
	fill_ix_array!(IXs, datas, cond, multi, nonmatches, groupby, cardinality)
end

findmatchix(cond::JoinCondition, aview, B, multi::typeof(identity)) = findall(i -> is_match(cond, aview, view(B, i)), eachindex(B))
findmatchix(cond::JoinCondition, aview, B, multi::typeof(first)) = let
    ix = findfirst(i -> is_match(cond, aview, view(B, i)), eachindex(B))
    isnothing(ix) ? [] : [ix]
end
findmatchix(cond::JoinCondition, aview, B, multi::typeof(last)) = let
    ix = findlast(i -> is_match(cond, aview, view(B, i)), eachindex(B))
    isnothing(ix) ? [] : [ix]
end



materialize_views(A::StructArray) = StructArray(map(materialize_views, StructArrays.components(A)))
materialize_views(A::ViewVector) = collect(A)
materialize_views(A::Vector{<:ViewVector}) = map(materialize_views, A)
materialize_views(A) = A

end
