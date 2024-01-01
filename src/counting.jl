normalize_cardinality(card::Integer) = ==(card)
normalize_cardinality(card::UnitRange{<:Integer}) = ∈(card)
normalize_cardinality(::typeof(+)) = >(0)
normalize_cardinality(::typeof(*)) = Returns(true)
normalize_cardinality(card::Function) = card

card_string(f::Returns{Bool}) = (@assert f.x; "any")
card_string(f::Base.Fix2{typeof(==)}) = string(f.x)
card_string(f::Base.Fix2{typeof(∈)}) = string(f.x)
card_string(f::Base.Fix2{typeof(>)}) = ">($(f.x))"

intermediate(f::Returns) = f
intermediate(f::Base.Fix2{typeof(==)}) = ≤(f.x)
intermediate(f::Base.Fix2{typeof(∈),<:AbstractRange}) = ≤(maximum(f.x))
intermediate(f::Base.Fix2{typeof(>)}) = Returns(true)

cardinality_check_intermediate(cnt, card) = intermediate(card)(cnt) || throw(ArgumentError("Cardinality exceeded: got $(_fmt_num(cnt)), expected $(card_string(card))"))
cardinality_check_final(cnt, card) = card(cnt) || throw(ArgumentError("Cardinality mismatch: got $(_fmt_num(cnt)), expected $(card_string(card))"))
_fmt_num(x) = x
_fmt_num(x::Integer) = Int(x)

create_cnts(datas, nonmatches, cardinality) = Base.Cartesian.@ntuple 2 i -> let
    T = min_cnt_type_promote(
        min_cnt_type_nonmatches(nonmatches[i]),
        min_cnt_type_cardinality(cardinality[3 - i]), # 3 - i because cardinality is reversed
    )
    map(Returns(create_zero(T)), datas[i])
end

create_zero(::Type{T}) where {T} = zero(T)
create_zero(::Type{Nothing}) = nothing

min_cnt_type_nonmatches(::typeof(drop)) = Nothing
min_cnt_type_nonmatches(::typeof(keep)) = Bool

min_cnt_type_cardinality(::Returns{Bool}) = Nothing
min_cnt_type_cardinality(f::Base.Fix2{typeof(in),<:AbstractVector}) = (@assert minimum(f.x) >= 0; min_cnt_type_cardinality(==(maximum(f.x))))
min_cnt_type_cardinality(f::Base.Fix2{typeof(in),<:Integer}) = min_cnt_type_cardinality(==(f.x))
min_cnt_type_cardinality(f::Base.Fix2{typeof(==),<:Integer}) = f.x == 0 ? Nothing : f.x == 1 ? Bool : (@assert 0 <= f.x < typemax(Int8); Int8)
min_cnt_type_cardinality(f::Base.Fix2{typeof(>),<:Integer}) = (@assert f.x == 0; Bool)

min_cnt_type_promote(::Type{Ta}, ::Type{Tb}) where {Ta, Tb} = sizeof(Ta) > sizeof(Tb) ? Ta : Tb

add_to_cnt!(cnts, ix, val, cardinality) = add_to_cnt!(eltype(cnts), cnts, ix, val, cardinality)
function add_to_cnt!(::Type{<:Integer}, cnts, ix, val, cardinality)
    cardinality_check_intermediate(cnts[ix] + val, cardinality)
    cnts[ix] = min(cnts[ix] + val, typemax(eltype(cnts)))
end
add_to_cnt!(::Type{Nothing}, cnts, ix, val, cardinality) = cardinality_check_intermediate(1, cardinality)
