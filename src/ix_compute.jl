function fill_ix_array!(mode, IXs, datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Union{Nothing, StaticInt{1}}, cardinality, cache)
    last_optimized = prepare_for_join(cache, mode, last(datas), cond, last(multi))
    _fill_ix_array!(mode, IXs, datas, cond, multi, nonmatches, groupby, cardinality, last_optimized)
end

function _fill_ix_array!(mode, IXs, datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Union{Nothing, StaticInt{1}}, cardinality, last_optimized)
    ix_seen_cnts = create_cnts(datas, nonmatches, cardinality)
    cnt = Ref(0)
    @inbounds for (ix_1, x_1) in pairs(first(datas))
        IX_2 = findmatchix(mode, cond, ix_1, x_1, last_optimized, last(multi))
        cnt[] = 0
        foreach_inbounds(IX_2) do ix_2
            cnt[] += 1
            add_to_cnt!(last(ix_seen_cnts), ix_2, true, first(cardinality))  # note that cardinality is reversed
        end
        add_to_cnt!(first(ix_seen_cnts), ix_1, cnt[], last(cardinality))  # note that cardinality is reversed
        @assert cardinality_ok(cnt[], last(cardinality))  # cnt[] is the final count, so it must be within the cardinality; add_to_cnt! should only check that cnt <= cardinality
        append_matchix!(IXs, (ix_1, IX_2), first(nonmatches), groupby)
    end
    @assert all(cnt -> cardinality_ok(cnt, first(cardinality)), last(ix_seen_cnts))  # note that cardinality is reversed
    append_nonmatchix!(IXs, ix_seen_cnts, nonmatches, groupby)
end

append_matchix!(IXs, (ix_1, IX_2), nonmatches, groupby::Nothing) = 
    foreach_inbounds(IX_2) do ix_2
        push!(IXs, (ix_1, ix_2))
    end
append_matchix!(IXs, (ix_1, IX_2), nonmatches::typeof(drop), groupby::StaticInt{1}) = isempty(IX_2) || push!(IXs, NoConvert((ix_1, IX_2)))
append_matchix!(IXs, (ix_1, IX_2), nonmatches::typeof(keep), groupby::StaticInt{1}) = push!(IXs, NoConvert((ix_1, IX_2)))

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::Nothing)
    for (ix_1, cnt) in pairs(ix_seen_cnts[1])
        cnt == 0 && push!(IXs, (ix_1, nothing))
    end
    IXs
end

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::Nothing)
    for (ix_2, cnt) in pairs(ix_seen_cnts[2])
        cnt == 0 && push!(IXs, (nothing, ix_2))
    end
    IXs
end

# these nonmatches are already appended
append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::StaticInt{1}) = IXs

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::StaticInt{1})
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    push!(IXs, NoConvert((nothing, IX_2)))
end

append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(drop)}, groupby) = IXs
function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(keep)}, groupby)
    append_nonmatchix!(IXs, ix_seen_cnts, (keep, drop), groupby)
    append_nonmatchix!(IXs, ix_seen_cnts, (drop, keep), groupby)
end


create_ix_array(datas, nonmatches, groupby::Nothing) = map(datas, reverse(nonmatches)) do X, nms
    empty_ix_vector(keytype(X), nms, Val(false))
end |> StructArray

create_ix_array(datas, nonmatches, groupby::StaticInt) = map(ntuple(identity, length(datas)), datas, reverse(nonmatches)) do i, X, nms
    empty_ix_vector(keytype(X), nms, Val(i != groupby))
end |> StructArray

empty_ix_vector(ix_T, nms::typeof(drop), group::Val{false}) = Vector{ix_T}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{false}) = Vector{Union{Nothing, ix_T}}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{false}) = Vector{Nothing}()
empty_ix_vector(ix_T, nms::typeof(drop), group::Val{true}) = VectorOfVectors{ix_T}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{true}) = VectorOfVectors{ix_T}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{true}) = Vector{EmptyVector{ix_T, Vector}}()
