import .NearestNeighbors as NN
using .StaticArrays: SVector


# by distance:
prepare_for_join(::Mode.Tree, X, cond::ByDistance) =
    (X, (cond.dist isa NN.MinkowskiMetric ? NN.KDTree : NN.BallTree)(map(cond.func_R, X) |> wrap_matrix, cond.dist), keytype(X)[], Float64[])
function findmatchix(::Mode.Tree, cond::ByDistance, ix_a, a, (B, tree, idx, _)::Tuple, multi::typeof(identity))
    NN.inrange_point!(tree, wrap_vector(cond.func_L(a)), cond.max, false, empty!(idx))
    idx
end
function findmatchix(::Mode.Tree, cond::ByDistance, ix_a, a, (B, tree, idx, dist)::Tuple, multi::Closest)
    resize!(idx, 1)
    resize!(dist, 1)
    NN.knn_point!(tree, wrap_vector(cond.func_L(a)), false, dist, idx, NN.always_false)
    cond.pred(only(dist), cond.max) ? idx : empty!(idx)
end

# by predicate:
prepare_for_join(::Mode.Tree, X, cond::ByPred{typeof((!) ∘ isdisjoint)}) =
    (X, NN.KDTree(map(wrap_vector ∘ endpoints ∘ cond.Rf, X) |> wrap_matrix, NN.Euclidean()), Int[])
function findmatchix(::Mode.Tree, cond::ByPred{typeof((!) ∘ isdisjoint)}, ix_a, a, (B, tree, idx)::Tuple, multi::typeof(identity))
    leftint = cond.Lf(a)
    inrange_rect!(tree, wrap_vector((-Inf, leftendpoint(leftint))), wrap_vector((rightendpoint(leftint), Inf)), empty!(idx))
    @p filter!(cond.pred(leftint, cond.Rf(B[_])), idx)
end


# helpers
wrap_matrix(X::Vector{<:AbstractVector}) = X
wrap_matrix(X::Vector{<:AbstractFloat}) = reshape(X, (1, :))
wrap_matrix(X::Vector{<:Integer}) = wrap_matrix(map(float, X))

wrap_vector(X::AbstractVector{<:Number}) = X
wrap_vector(X::Number) = MaybeVector{typeof(X)}(X)
wrap_vector(t::Tuple) = SVector(t)



# until https://github.com/KristofferC/NearestNeighbors.jl/pull/150
using .NearestNeighbors: KDTree, isleaf, get_leaf_range, getleft, getright

function inrect(tree, a, b)
    idx = Int[]
    inrange_rect!(tree, a, b, idx)
    return idx
end

function inrange_rect!(tree::KDTree, a, b, idxs, index=1)
    if isleaf(tree.tree_data.n_internal_nodes, index)
        for z in get_leaf_range(tree.tree_data, index)
            idx = tree.reordered ? z : tree.indices[z]
            all(a .<= tree.data[idx] .<= b) && push!(idxs, tree.indices[z])
        end
    else
        (; split_val, split_dim) = tree.nodes[index]
        a[split_dim] <= split_val && inrange_rect!(tree, a, b, idxs,  getleft(index))
        b[split_dim] >= split_val && inrange_rect!(tree, a, b, idxs, getright(index))
    end
end
