module FlexiJoins

using StructArrays
using Static: StaticInt, known
using Accessors
using DataPipes
using Indexing
using SplitApplyCombine: mapview
using IntervalSets
import NearestNeighbors as NN


export
    innerjoin, leftjoin, rightjoin, outerjoin,
    flexijoin, joinindices, materialize_views, @optic,
    by_key, by_distance, by_pred,
    keep, drop, closest


include("nothingindex.jl")
include("conditions.jl")
include("bykey.jl")
include("bydistance.jl")
include("bypredicate.jl")
include("normalize_specs.jl")
include("ix_compute.jl")

_commondoc = """
Join two datasets, `A` and `B`, by the `by` condition.
"""


"""    innerjoin((;A, B), by; [nonmatches=drop], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
innerjoin(args...; kwargs...) = flexijoin(args...; kwargs...)
"""    leftjoin((;A, B), by; [nonmatches=(keep, drop)], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
leftjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> i == 1 ? keep : drop, length(datas)), kwargs...)
"""    rightjoin((;A, B), by; [nonmatches=(drop, keep)], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
rightjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> i == 1 ? drop : keep, length(datas)), kwargs...)
"""    outerjoin((;A, B), by; [nonmatches=keep], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
outerjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> keep, length(datas)), kwargs...)


"""    flexijoin((;A, B), by; [nonmatches=drop], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
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
        normalize_arg(cond, datas),
        normalize_arg(get(kwargs, :multi, nothing), datas; default=identity),
        normalize_arg(get(kwargs, :nonmatches, nothing), datas; default=drop),
        normalize_groupby(get(kwargs, :groupby, nothing), datas),
        normalize_arg(get(kwargs, :cardinality, nothing), datas; default=*),
        get(kwargs, :mode, nothing),
    )
end

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode)
    mode = @something(mode, best_mode(cond, datas))
    if any(@. multi !== identity && nonmatches !== drop)
        error("Values of arguments don't make sense together: ", (; nonmatches, multi))
    end
	IXs = create_ix_array(datas, nonmatches, groupby)
	fill_ix_array!(mode, IXs, datas, cond, multi, nonmatches, groupby, cardinality)
end


materialize_views(A::StructArray) = StructArray(map(materialize_views, StructArrays.components(A)))
materialize_views(A::ViewVector) = collect(A)
materialize_views(A::Vector{<:ViewVector}) = map(materialize_views, A)
materialize_views(A) = A

end
