module FlexiJoins

using StructArrays
using Accessors
using DataPipes
using FlexiMaps: mapview, _eltype
using SentinelViews: sentinelview, SentinelView
using IntervalSets
import DataAPI: innerjoin, leftjoin, rightjoin, outerjoin
using ArraysOfArrays: VectorOfVectors


export
    innerjoin, leftjoin, rightjoin, outerjoin,
    flexijoin, joinindices, materialize_views,
    by_key, by_distance, by_pred, not_same,
    keep, drop, closest,
    join_cache


include("utils.jl")
include("conditions.jl")
include("normalize_specs.jl")
include("counting.jl")
include("prepare_cache.jl")
include("bykey.jl")
include("bydistance.jl")
include("bypredicate.jl")
include("bysame.jl")
include("ix_compute.jl")
include("joins.jl")

using StaticArrays: SVector
import NearestNeighbors as NN
include("nearestneighbors.jl")

end
