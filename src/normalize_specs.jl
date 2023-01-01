struct Drop end
struct Keep end
const drop = Drop()
const keep = Keep()
Base.broadcastable(x::Union{Drop, Keep}) = Ref(x)
Base.show(io::IO, x::Keep) = write(io, "keep")
Base.show(io::IO, x::Drop) = write(io, "drop")


Base.in(x::Integer, ::typeof(*)) = true
Base.in(x::Integer, ::typeof(+)) = x > 0


stripname_groupby(x::Nothing, datas) = x
stripname_groupby(x::Symbol, datas::NamedTuple{NS}) where {NS} = StaticInt(findfirst(==(x), NS))

stripnames(::Nothing, datas; default) = map(Returns(default), datas) |> values

stripnames(x, datas; default) = map(Returns(x), datas) |> values

stripnames(x::NamedTuple{N, <:Tuple{Any}}, datas::NamedTuple{NS}; default) where {N, NS} = let
	@assert only(N) ∈ NS
	ix = findfirst(==(only(N)), NS)
	ntuple(i -> i == ix ? only(x) : default, length(NS))
end

stripnames(x::NamedTuple{NSx}, datas::NamedTuple{NS}; default) where {NSx, NS} = let
	@assert NSx ⊆ NS
	merge(
		map(Returns(default), datas),
		x
	) |> values
end
