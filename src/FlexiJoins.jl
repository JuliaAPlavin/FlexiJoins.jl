module FlexiJoins

using StructArrays
using Accessors
using DataPipes
using ArraysExtra: mapview, sentinelview, SentinelView
using IntervalSets
import DataAPI: innerjoin, leftjoin, rightjoin, outerjoin
using ArraysOfArrays: VectorOfVectors
using Requires


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


function __init__()
    @require DataFrames = "a93c6f00-e57d-5684-b7b6-d8193f3e46c0" begin
        using .DataFrames: DataFrame
        using Tables: columntable

        function _flexijoin(datas::Tuple{DataFrame, DataFrame}, args...; kwargs...)
            datas = map(to_table_for_join, datas)
            res = innerjoin(datas, args...; kwargs...)
            return hcat(map(to_df_joined, StructArrays.components(res))...; makeunique=true)
        end

        to_table_for_join(xs::DataFrame) = StructArray(columntable(xs))
        to_df_joined(xs::AbstractArray) = DataFrame(xs)
        function to_df_joined(xs::SentinelView)
            base_eltype = eltype(parent(xs))
            empty_row = constructorof(base_eltype)(ntuple(_ -> missing, fieldcount(base_eltype))...)
            DataFrame(x isa base_eltype ? x : empty_row for x in xs)
        end
    end
end

end
