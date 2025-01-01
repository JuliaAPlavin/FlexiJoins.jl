_commondoc = """
Join two datasets, `A` and `B`, by the `by` condition.
"""


"""    innerjoin((;A, B), by; [nonmatches=drop], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
innerjoin(args...; kwargs...) = flexijoin(args...; kwargs...)
"""    leftjoin((;A, B), by; [nonmatches=(keep, drop)], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
leftjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> i == 1 ? keep : drop, length(datas)), kwargs...)
"""    rightjoin((;A, B), by; [nonmatches=(drop, keep)], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
rightjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> i == 1 ? drop : keep, length(datas)), kwargs...)
"""    outerjoin((;A, B), by; [nonmatches=keep], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
"""
outerjoin(datas, args...; kwargs...) = flexijoin(datas, args...; nonmatches=ntuple(i -> keep, length(datas)), kwargs...)

"""    flexijoin((;A, B), by; [nonmatches=drop], [multi], [groupby], [cardinality=(*, *)])
$_commondoc
Performs an inner join by default. See also the `innerjoin()` function.
"""
flexijoin(datas, args...; kwargs...) = _flexijoin(datas, args...; kwargs...)

function _flexijoin(datas, cond; kwargs...)
    IXs = joinindices(datas, cond; kwargs...)
    myview(datas, IXs)
end

function joinindices(datas::NamedTuple{NS}, cond; kwargs...) where {NS}
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(NamedTuple{NS}(StructArrays.components(IXs_unnamed)))
end

function joinindices(datas::Tuple, cond; kwargs...)
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(StructArrays.components(IXs_unnamed))
end

function _joinindices(datas, cond; multi=nothing, nonmatches=nothing, groupby=nothing, cardinality=nothing, mode=nothing, cache=nothing, loop_over_side=nothing)
    _joinindices(
        values(datas),
        normalize_arg(cond, datas),
        normalize_arg(multi, datas; default=identity),
        normalize_arg(nonmatches, datas; default=drop),
        normalize_joinside(groupby, datas),
        map(normalize_cardinality, normalize_arg(cardinality, datas; default=*)),
        mode,
        cache,
        normalize_joinside(loop_over_side, datas),
    )
end

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode, cache, loop_over_side::Nothing)
    loop_over_side = which_side_first(datas, cond, multi, nonmatches, groupby, cardinality, mode)
    _joinindices(datas, cond, multi, nonmatches, groupby, cardinality, mode, cache, loop_over_side)
end

_joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode, cache, loop_over_side::Val{2}) = 
    _joinindices(
        swap_sides(datas),
        swap_sides(cond),
        swap_sides(multi),
        swap_sides(nonmatches),
        swap_sides(groupby),
        swap_sides(cardinality),
        mode,
        cache,
        Val(1),
    ) |> swap_sides

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode, cache, loop_over_side::Val{1})
    if any(@. multi !== identity && nonmatches !== drop)
        error("Values of arguments don't make sense together: ", (; nonmatches, multi))
    end

    mode = choose_mode(mode, cond, datas)
    isnothing(mode) && error("No known mode supported by $cond\nPass mode=FlexiJoins.Mode.NestedLoop() for nested loop join that always works but performs O(n*m) comparisons.\n")
    IXs = create_ix_array(datas, nonmatches, groupby)
    fill_ix_array!(mode, IXs, datas, cond, multi, nonmatches, groupby, cardinality, cache)
end

function which_side_first(datas, cond, multi::Tuple{typeof(identity), typeof(identity)}, nonmatches, groupby::Nothing, cardinality, mode)
    mode_1 = choose_mode(mode, cond, datas)
    mode_2 = choose_mode(mode, swap_sides(cond), swap_sides(datas))
    if !isnothing(mode_1) && !isnothing(mode_2)
        preferred_first_side(datas, cond, (mode_1, mode_2))
    elseif !isnothing(mode_1)
        Val(1)
    elseif !isnothing(mode_2)
        Val(2)
    else
        error("No known mode supported by $cond\nPass mode=FlexiJoins.Mode.NestedLoop() for nested loop join that always works but performs O(n*m) comparisons.\n")
    end
end
which_side_first(datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Nothing, cardinality, mode) = Val(1)
which_side_first(datas, cond, multi::Tuple{Any, typeof(identity)}, nonmatches, groupby::Nothing, cardinality, mode) = Val(2)
which_side_first(datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Val{1}, cardinality, mode) = Val(1)
which_side_first(datas, cond, multi::Tuple{Any, typeof(identity)}, nonmatches, groupby::Val{2}, cardinality, mode) = Val(2)
which_side_first(datas, cond, multi, nonmatches, groupby, cardinality, mode) = error("Unsupported parameter combination")

preferred_first_side(datas, cond, modes::Tuple{M, M}) where {M} = preferred_first_side(datas, cond, first(modes))
preferred_first_side(datas, cond, _) = Val(findmax(length, datas)[2])
