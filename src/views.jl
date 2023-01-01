struct SentinelView{T, N, A, I, TS} <: AbstractArray{T, N}
    parent::A
    indices::I
    sentinel::TS
end

SentinelView(a, indices, sentinel) =
    SentinelView{
        Union{eltype(a), typeof(sentinel)},
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
