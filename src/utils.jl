const VIEWTYPES = Union{SubArray, SentinelView}

materialize_views(A::AbstractArray) = map(materialize_views, A)
materialize_views(A::StructArray) = StructArray(map(materialize_views, StructArrays.components(A)))
materialize_views(A) = A

myview(A, I::AbstractArray) = sentinelview(A, I, nothing)
myview(A, Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)
myview(A::NamedTuple{NS}, I::StructArray{<:NamedTuple}) where {NS} =
    if any(∈(NS), (:_, :__, :___))
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

firstn_by!(A::AbstractVector, n=1; by) = view(partialsort!(A, 1:min(n, length(A)); by), 1:min(n, length(A)))


# workaround for https://github.com/JuliaArrays/StructArrays.jl/issues/228
struct NoConvert{T}
    value::T
end
StructArrays.maybe_convert_elt(::Type{T}, vals::NoConvert) where {T} = vals.value
