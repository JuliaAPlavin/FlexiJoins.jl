const VIEWTYPES = Union{SubArray, SentinelView}


myview(A, I::AbstractArray) = sentinelview(A, I, nothing)
myview(A::VIEWTYPES, I::AbstractArray) = myview(parent(A), only(parentindices(A))[I])
myview(A::VIEWTYPES, Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)  # same as below, for disambiguation
myview(A,            Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)
myview(A::NamedTuple{NS}, I::StructArray{<:NamedTuple}) where {NS} =
    if :_ ∈ NS
        merge(
            map(NS) do k
                if k ∈ (:_, :__, :___)
                    map(StructArrays.components(A[k])) do A
                        myview(A, StructArrays.component(I, k))
                    end
                else
                    NamedTuple{(k,)}((myview(A[k], StructArrays.component(I, k)),))
                end
            end...
        ) |> StructArray
    else
        map(A, StructArrays.components(I)) do A, I
            myview(A, I)
        end |> StructArray
    end
myview(A::Tuple, I::StructArray{<:Tuple}) =
    map(A, StructArrays.components(I)) do A, I
        myview(A, I)
    end |> StructArray



# from https://github.com/andyferris/AcceleratedArrays.jl/blob/master/src/MaybeVector.jl
struct MaybeVector{T} <: AbstractVector{T}
    length::Int8
    data::T

    MaybeVector{T}() where {T} = new{T}(0)
    MaybeVector{T}(x::T) where {T} = new{T}(1, x)
end

Base.size(a::MaybeVector) = (a.length,)
Base.IndexStyle(::Type{<:MaybeVector}) = IndexLinear()
Base.@propagate_inbounds function Base.getindex(a::MaybeVector, i::Integer)
    @boundscheck checkbounds(a, i)
    return a.data
end


# somehow, simple iteration of a view calls checkbounds...?
@inline foreach_inbounds(f, A::AbstractArray) = for i in eachindex(A)
    f(@inbounds A[i])
end
@inline foreach_inbounds(f, A) = for a in A
    f(a)
end

# "view" that works with array of indices (regular view) and with iterable of indices (iterator)
_do_view(A, I::AbstractArray) = @view A[I]
_do_view(A, I) = @p I |> Iterators.map(A[_])

firstn_by!(A::AbstractVector, n=1; by) = view(partialsort!(A, 1:min(n, length(A)); by), 1:min(n, length(A)))

# Base.eltype returns Any for mapped/flattened iterators
_eltype(A::AbstractArray) = eltype(A)
_eltype(A::T) where {T} = Core.Compiler.return_type(first, Tuple{T})


# workaround for https://github.com/JuliaArrays/StructArrays.jl/issues/228
struct NoConvert{T}
    value::T
end
StructArrays.maybe_convert_elt(::Type{T}, vals::NoConvert) where {T} = vals.value
