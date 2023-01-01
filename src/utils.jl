struct SentinelView{T, N, A, I, TS} <: AbstractArray{T, N}
    parent::A
    indices::I
    sentinel::TS
end

function SentinelView(a, indices, sentinel)
    @assert !(typeof(sentinel) <: keytype(a))
    SentinelView{
        if eltype(indices) <: keytype(a)
            valtype(a)
        elseif eltype(indices) <: Union{keytype(a), typeof(sentinel)}
            Union{valtype(a), typeof(sentinel)}
        else
            error()
        end,
        ndims(indices),
        typeof(a),
        typeof(indices),
        typeof(sentinel)
    }(a, indices, sentinel)
end

Base.IndexStyle(::Type{SentinelView{T, N, A, I}}) where {T, N, A, I} = IndexStyle(I)
Base.axes(a::SentinelView) = axes(a.indices)
Base.size(a::SentinelView) = size(a.indices)

Base.@propagate_inbounds function Base.getindex(a::SentinelView, is::Int...)
    I = a.indices[is...]
    I === a.sentinel ? a.sentinel : a.parent[I]
end

Base.parent(a::SentinelView) = a.parent
Base.parentindices(a::SentinelView) = (a.indices,)


myview(A, I::AbstractArray) = SentinelView(A, I, nothing)
myview(A::SentinelView, I::AbstractArray) = myview(parent(A), only(parentindices(A))[I])
myview(A::SentinelView, Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)  # same as below, for disambiguation
myview(A,               Is::AbstractArray{<:AbstractArray}) = map(I -> myview(A, I), Is)
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


materialize_views(A::StructArray) = StructArray(map(materialize_views, StructArrays.components(A)))
materialize_views(A::SentinelView) = collect(A)
materialize_views(A::Vector{<:SentinelView}) = map(materialize_views, A)
materialize_views(A) = A



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
