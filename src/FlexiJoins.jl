module FlexiJoins

using StructArrays
using Tables: columntable
using Static: StaticInt, static
using Accessors
using DataPipes
using SplitApplyCombine: mapview
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


function __init__()
    @require DataFrames = "a93c6f00-e57d-5684-b7b6-d8193f3e46c0" begin
        using .DataFrames: DataFrame

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

    @require NearestNeighbors = "b8a86587-4115-5ab1-83bc-aa920d37bbce" begin
        @require StaticArrays = "90137ffa-7385-5640-81b9-e52037218182" include("nearestneighbors.jl")
    end
end

end
