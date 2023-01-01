struct Drop end
struct Keep end
const drop = Drop()
const keep = Keep()
Base.broadcastable(x::Union{Drop, Keep}) = Ref(x)
Base.show(io::IO, x::Keep) = write(io, "keep")
Base.show(io::IO, x::Drop) = write(io, "drop")


normalize_groupby(x::Nothing, datas) = x
normalize_groupby(x::Symbol, datas::NamedTuple{NS}) where {NS} = StaticInt(findfirst(==(x), NS))

normalize_arg(::Nothing, datas; default) = map(Returns(default), datas) |> values

normalize_arg(x, datas; default) = map(Returns(x), datas) |> values

normalize_arg(x::Tuple, datas::Union{NamedTuple, Tuple}; default) = let
	@assert length(x) == length(datas)
	x
end

normalize_arg(x::NamedTuple{N, <:Tuple{Any}}, datas::NamedTuple{NS}; default) where {N, NS} = let
	@assert only(N) ∈ NS
	ix = findfirst(==(only(N)), NS)
	ntuple(i -> i == ix ? only(x) : default, length(NS))
end

normalize_arg(x::NamedTuple{NSx}, datas::NamedTuple{NS}; default) where {NSx, NS} = let
	@assert NSx ⊆ NS
	merge(
		map(Returns(default), datas),
		x
	) |> values
end


swap_sides(x::Nothing) = x
swap_sides(x::Tuple) = reverse(x)
swap_sides(x::NamedTuple{NS}) where {NS} = NamedTuple{reverse(NS)}(reverse(values(x)))
swap_sides(x::StructArray) = StructArray(swap_sides(StructArrays.components(x)))
