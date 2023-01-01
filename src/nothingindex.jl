struct NothingIndex end

Base.length(::NothingIndex) = 1
Base.getindex(A::AbstractArray, ::NothingIndex) = nothing
Base.promote_rule(::Type{NothingIndex}, T::Type) = Union{NothingIndex, T}
Base.checkindex(::Type{Bool}, inds::AbstractUnitRange, ix::NothingIndex) = true
Indexing.ViewArray(a, indices::AbstractArray{Union{NothingIndex, TI}}) where {TI} = ViewArray{Union{Indexing._valtype(a), Nothing}}(a, indices)


myview(A::AbstractArray, I::AbstractArray) = ViewArray(A, I)
myview(A::AbstractArray, Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)
myview(A::NamedTuple, I::StructArray{<:NamedTuple}) =
    map(A, StructArrays.components(I)) do A, I
        myview(A, I)
    end |> StructArray
myview(A::Tuple, I::StructArray{<:Tuple}) =
    map(A, StructArrays.components(I)) do A, I
        myview(A, I)
    end |> StructArray
