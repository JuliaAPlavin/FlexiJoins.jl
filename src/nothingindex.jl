struct NothingIndex end

Base.length(::NothingIndex) = 1
Base.getindex(A::AbstractArray, ::NothingIndex) = nothing
Base.promote_rule(::Type{NothingIndex}, T::Type) = Union{NothingIndex, T}
Base.checkindex(::Type{Bool}, inds::AbstractUnitRange, ix::NothingIndex) = true
Indexing.ViewArray(a, indices::AbstractArray{Union{NothingIndex, TI}}) where {TI} = ViewArray{Union{Indexing._valtype(a), Nothing}}(a, indices)


myview(A, I::AbstractVector) = ViewArray(A, I)
myview(A, Is::AbstractVector{<:AbstractVector}) = map(I -> myview(A, I), Is)
