struct Drop end
struct Keep end
const drop = Drop()
const keep = Keep()
Base.broadcastable(x::Union{Drop, Keep}) = Ref(x)
Base.show(io::IO, x::Keep) = write(io, "keep")
Base.show(io::IO, x::Drop) = write(io, "drop")


normalize_joinside(x::Nothing, datas) = x
normalize_joinside(x::Integer, datas::Union{Tuple, NamedTuple}) = Val(x)
normalize_joinside(x::Symbol, datas::NamedTuple{NS}) where {NS} = Val(@something(findfirst(==(x), NS), error("NamedTuple{$NS} has no field $x")))

normalize_arg(::Nothing, datas; default) = ntuple(Returns(default), length(datas))
normalize_arg(x::JoinCondition, datas) = x
normalize_arg(x, datas; default) = ntuple(Returns(x), length(datas))
normalize_arg(x::Tuple, datas::Union{NamedTuple, Tuple}; default) = (@assert length(x) == length(datas); x)
normalize_arg(x::NamedTuple{NSx}, datas::NamedTuple{NS}; default) where {NSx, NS} = let
    @assert NSx ⊆ NS
    map(n -> get(x, n, default), NS)
end


swap_sides(x::Nothing) = x
swap_sides(x::Tuple) = reverse(x)
swap_sides(x::NamedTuple{NS}) where {NS} = NamedTuple{reverse(NS)}(reverse(values(x)))
swap_sides(x::StructArray) = StructArray(swap_sides(StructArrays.components(x)))
swap_sides(x::Val{1}) = Val(2)
swap_sides(x::Val{2}) = Val(1)
