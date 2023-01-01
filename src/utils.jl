struct SentinelView{T, N, A, I, TS} <: AbstractArray{T, N}
    parent::A
    indices::I
    sentinel::TS
end

SentinelView(a, indices, sentinel) =
    SentinelView{
        if eltype(indices) <: keytype(a)
            eltype(a)
        elseif eltype(indices) <: Union{typeof(sentinel), keytype(a)}
            Union{eltype(a), typeof(sentinel)}
        else
            error()
        end,
        ndims(indices),
        typeof(a),
        typeof(indices),
        typeof(sentinel)
    }(a, indices, sentinel)

Base.IndexStyle(::Type{SentinelView{T, N, A, I}}) where {T, N, A, I} = IndexStyle(I)
Base.axes(a::SentinelView) = axes(a.indices)
Base.size(a::SentinelView) = size(a.indices)

Base.@propagate_inbounds function Base.getindex(a::SentinelView, is::Int...)
    I = a.indices[is...]
    I === a.sentinel ? a.sentinel : a.parent[I]
end

Base.parent(a::SentinelView) = a.parent
Base.parentindices(a::SentinelView) = a.indices


myview(A, I::AbstractArray) = SentinelView(A, I, nothing)
myview(A, Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)
myview(A::NamedTuple, I::StructArray{<:NamedTuple}) =
    map(A, StructArrays.components(I)) do A, I
        myview(A, I)
    end |> StructArray
myview(A::Tuple, I::StructArray{<:Tuple}) =
    map(A, StructArrays.components(I)) do A, I
        myview(A, I)
    end |> StructArray



# from https://github.com/andyferris/AcceleratedArrays.jl/blob/master/src/MaybeVector.jl
struct MaybeVector{T} <: AbstractVector{T}
    length::UInt8
    data::T

    MaybeVector{T}() where {T} = new{T}(0x00)
    MaybeVector{T}(x::T) where {T} = new{T}(0x01, x)
end

Base.axes(a::MaybeVector) = (Base.OneTo(a.length),)
Base.size(a::MaybeVector) = (a.length,)
Base.IndexStyle(::Type{<:MaybeVector}) = IndexLinear()
Base.@propagate_inbounds function Base.getindex(a::MaybeVector, i::Integer)
    @boundscheck if a.length != 0x01 || i != 1
        throw(BoundsError(a, i))
    end
    return a.data
end
Base.@propagate_inbounds function Base.getindex(a::MaybeVector)
    @boundscheck if a.length != 0x01
        throw(BoundsError(a, i))
    end
    return a.data
end
