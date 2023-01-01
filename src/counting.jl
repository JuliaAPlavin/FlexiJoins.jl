cardinality_ok(cnt::Integer, card::Integer) = cnt == card
cardinality_ok(cnt::Integer,   ::typeof(*)) = true
cardinality_ok(cnt::Nothing,   ::typeof(*)) = true
cardinality_ok(cnt::Integer,   ::typeof(+)) = cnt > 0


create_cnts(datas, nonmatches, cardinality) = ntuple(2) do i
    T = min_cnt_type_promote(
        min_cnt_type_nonmatches(nonmatches[i]),
        min_cnt_type_cardinality(cardinality[i]),
    )
    map(Returns(create_zero(T)), datas[i])
end

create_zero(::Type{T}) where {T} = zero(T)
create_zero(::Type{Nothing}) = nothing

min_cnt_type_nonmatches(::typeof(drop)) = Nothing
min_cnt_type_nonmatches(::typeof(keep)) = Bool
min_cnt_type_cardinality(::typeof(*)) = Nothing
min_cnt_type_cardinality(::typeof(+)) = Bool
min_cnt_type_cardinality(x::Integer) = x == 0 ? Nothing : x == 1 ? Bool : (@assert 0 <= x < typemax(Int8); Int8)
min_cnt_type_promote(::Type{Ta}, ::Type{Tb}) where {Ta, Tb} = sizeof(Ta) > sizeof(Tb) ? Ta : Tb

add_to_cnt!(cnts, ix, val, cardinality) = add_to_cnt!(_valtype(cnts), cnts, ix, val, cardinality)
function add_to_cnt!(::Type{<:Integer}, cnts, ix, val, cardinality)
    @assert cardinality_ok(cnts[ix] + val, cardinality)
    cnts[ix] = min(cnts[ix] + val, typemax(_valtype(cnts)))
end
add_to_cnt!(::Type{Nothing}, cnts, ix, val, cardinality) = nothing
add_to_cnt!(::Type{Nothing}, cnts, ix, val, cardinality::Integer) = @assert cardinality != 0

@inline _valtype(X) = eltype(values(X))  # don't pirate Base.valtype
