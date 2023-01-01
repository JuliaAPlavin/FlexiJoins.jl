module FlexiJoins

using StructArrays
using Static: StaticInt, static
using Accessors
using DataPipes
using SplitApplyCombine: mapview
using IntervalSets
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

end
