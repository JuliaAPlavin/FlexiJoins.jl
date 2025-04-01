module SkyCoordsExt
using SkyCoords
using FlexiJoins
using FlexiJoins: ByDistance, Mode, NN, Closest, wrap_vector, wrap_matrix, mapview, is_match

separation_to_chord(x) = 2 * sin(x / 2)

const _ByDistance_sep = ByDistance{<:Any, <:Any, typeof(separation)}
const _post_func = vec ∘ CartesianCoords{ICRSCoords}

FlexiJoins.prepare_for_join(::Mode.Tree, X, cond::_ByDistance_sep) =
    (
        X,
        NN.KDTree(map(_post_func ∘ cond.func_R, X) |> wrap_matrix, NN.Distances.Euclidean()),
        separation_to_chord(cond.max)
    )

FlexiJoins.findmatchix(::Mode.Tree, cond::_ByDistance_sep, ix_a, a, (B, tree, maxchord)::Tuple, multi::typeof(identity)) =
    NN.inrange(tree, wrap_vector(_post_func(cond.func_L(a))), maxchord)

function FlexiJoins.findmatchix(::Mode.Tree, cond::_ByDistance_sep, ix_a, a, (B, tree, maxchord)::Tuple, multi::Closest)
    idxs, dists = NN.knn(tree, wrap_vector(_post_func(cond.func_L(a))), 1)
    cond.pred(only(dists), maxchord) ? idxs : empty!(idxs)
end


# to support mode=Sort():
FlexiJoins.sort_byf(cond::_ByDistance_sep) = SkyCoords.lat ∘ cond.func_R

function FlexiJoins.searchsorted_matchix(cond::_ByDistance_sep, a, B, perm)
    sf = FlexiJoins.sort_byf(cond)
    arr = mapview(i -> sf(@inbounds B[i]), perm)
    val = SkyCoords.lat(cond.func_L(a))
    P = @view perm[searchsortedfirst(arr, val - cond.max):searchsortedlast(arr, val + cond.max)]
    return filter(i -> is_match(cond, a, @inbounds B[i]), P)
end

end
