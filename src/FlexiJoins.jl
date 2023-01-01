module FlexiJoins

using StructArrays
using Static: StaticInt
using Accessors
using DataPipes
using SplitApplyCombine: mapview
using IntervalSets
using MicroCollections: vec1
import NearestNeighbors as NN
import DataAPI: innerjoin, leftjoin, rightjoin, outerjoin
using ArraysOfArrays: VectorOfVectors


export
    innerjoin, leftjoin, rightjoin, outerjoin,
    flexijoin, joinindices, materialize_views, @optic,
    by_key, by_distance, by_pred, not_same,
    keep, drop, closest,
    join_cache


include("utils.jl")
include("normalize_specs.jl")
include("counting.jl")
include("conditions.jl")
include("prepare_cache.jl")
include("bykey.jl")
include("bydistance.jl")
include("bypredicate.jl")
include("bysame.jl")
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

function _joinindices(datas, cond; multi=nothing, nonmatches=nothing, groupby=nothing, cardinality=nothing, mode=nothing, cache=nothing)
    _joinindices(
        values(datas),
        normalize_arg(cond, datas),
        normalize_arg(multi, datas; default=identity),
        normalize_arg(nonmatches, datas; default=drop),
        normalize_groupby(groupby, datas),
        normalize_arg(cardinality, datas; default=*),
        mode,
        cache,
    )
end

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode, cache)
    first_side = which_side_first(datas, cond, multi, nonmatches, groupby, cardinality, mode)
    if first_side == 2
        return _joinindices(
            swap_sides(datas),
            swap_sides(cond),
            swap_sides(multi),
            swap_sides(nonmatches),
            swap_sides(groupby),
            swap_sides(cardinality),
            mode,
            cache,
        ) |> swap_sides
    end
    @assert first_side == 1

    if any(@. multi !== identity && nonmatches !== drop)
        error("Values of arguments don't make sense together: ", (; nonmatches, multi))
    end

    mode = choose_mode(mode, cond, datas)
    isnothing(mode) && error("No known mode supported by $cond")
	IXs = create_ix_array(datas, nonmatches, groupby)
	fill_ix_array!(mode, IXs, datas, cond, multi, nonmatches, groupby, cardinality, cache)
end

function which_side_first(datas, cond, multi::Tuple{typeof(identity), typeof(identity)}, nonmatches, groupby::Nothing, cardinality, mode)
    mode_1 = choose_mode(mode, cond, datas)
    mode_2 = choose_mode(mode, swap_sides(cond), swap_sides(datas))
    if !isnothing(mode_1) && !isnothing(mode_2)
        preferred_first_side(datas, cond, (mode_1, mode_2))
    elseif !isnothing(mode_1)
        StaticInt(1)
    elseif !isnothing(mode_2)
        StaticInt(2)
    else
        error("No known mode supported by $cond")
    end
end
which_side_first(datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Nothing, cardinality, mode) = StaticInt(1)
which_side_first(datas, cond, multi::Tuple{Any, typeof(identity)}, nonmatches, groupby::Nothing, cardinality, mode) = StaticInt(2)
which_side_first(datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::StaticInt{1}, cardinality, mode) = StaticInt(1)
which_side_first(datas, cond, multi::Tuple{Any, typeof(identity)}, nonmatches, groupby::StaticInt{2}, cardinality, mode) = StaticInt(2)
which_side_first(datas, cond, multi, nonmatches, groupby, cardinality, mode) = error("Unsupported parameter combination")

preferred_first_side(datas, cond, modes::Tuple{M, M}) where {M} = preferred_first_side(datas, cond, first(modes))
preferred_first_side(datas, cond, mode) = StaticInt(1)

end
