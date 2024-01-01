module DataFramesExt
using DataFrames: DataFrame, AbstractDataFrame
using DataFrames.Tables: columntable
using FlexiJoins.StructArrays: StructArrays, StructArray
using FlexiJoins: sentinelview, SentinelView
using FlexiJoins: constructorof
import FlexiJoins: _flexijoin

function _flexijoin(datas::Tuple{<:AbstractDataFrame, <:AbstractDataFrame}, args...; kwargs...)
    datas = map(to_table_for_join, datas)
    res = _flexijoin(datas, args...; kwargs...)
    return hcat(map(to_df_joined, StructArrays.components(res))...; makeunique=true)
end

to_table_for_join(xs::AbstractDataFrame) = StructArray(columntable(xs))
to_df_joined(xs::AbstractArray) = DataFrame(xs)
function to_df_joined(xs::SentinelView)
    base_eltype = eltype(parent(xs))
    empty_row = constructorof(base_eltype)(ntuple(_ -> missing, fieldcount(base_eltype))...)
    DataFrame(x isa base_eltype ? x : empty_row for x in xs)
end

end
