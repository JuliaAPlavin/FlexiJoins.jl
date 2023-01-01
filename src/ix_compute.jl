function fill_ix_array!(IXs, datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Union{Nothing, StaticInt{1}}, cardinality)
	ix_seen_cnts = map(datas) do data
		map(Returns(0), data)
	end
    last_optimized = optimize(last, datas, cond, last(multi))
	for (ix_1, x_1) in pairs(first(datas))
		IX_2 = findmatchix(cond, x_1, last_optimized, last(multi))
		ix_seen_cnts[1][ix_1] += length(IX_2)
		@assert length(IX_2) ∈ cardinality[2]
		for ix_2 in IX_2
			ix_seen_cnts[2][ix_2] += 1
		end
        append_matchix!(IXs, (ix_1, IX_2), first(nonmatches), groupby)
	end
    @assert all(∈(cardinality[2]), ix_seen_cnts[2])
	append_nonmatchix!(IXs, ix_seen_cnts, nonmatches, groupby)
end

append_matchix!(IXs, (ix_1, IX_2), nonmatches, groupby::Nothing) =
    for ix_2 in IX_2
        push!(IXs, (ix_1, ix_2))
    end

append_matchix!(IXs, (ix_1, IX_2), nonmatches::typeof(drop), groupby::StaticInt{1}) = isempty(IX_2) || push!(IXs, (ix_1, IX_2))
append_matchix!(IXs, (ix_1, IX_2), nonmatches::typeof(keep), groupby::StaticInt{1}) = push!(IXs, (ix_1, IX_2))

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::Nothing)
    IX_1 = @p ix_seen_cnts[1] |> findall(==(0))
    for ix_1 in IX_1
        push!(IXs, (ix_1, NothingIndex()))
    end
    IXs
end

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::Nothing)
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    for ix_2 in IX_2
        push!(IXs, (NothingIndex(), ix_2))
    end
    IXs
end

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::StaticInt{1})
    # these nonmatches are already appended
    IXs
end

function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::StaticInt{1})
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    push!(IXs, (NothingIndex(), IX_2))
    IXs
end

append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(drop)}, groupby) = IXs
function append_nonmatchix!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(keep)}, groupby)
    append_nonmatchix!(IXs, ix_seen_cnts, (keep, drop), groupby)
    append_nonmatchix!(IXs, ix_seen_cnts, (drop, keep), groupby)
end


create_ix_array(datas, nonmatches, groupby::Nothing) = map(datas, reverse(nonmatches)) do X, nms
    empty_ix_vector(eltype(keys(X)), nms, Val(false))
end |> StructArray

create_ix_array(datas, nonmatches, groupby::StaticInt) = map(ntuple(identity, length(datas)), datas, reverse(nonmatches)) do i, X, nms
    empty_ix_vector(eltype(keys(X)), nms, Val(i != known(groupby)))
end |> StructArray

empty_ix_vector(ix_T, nms::typeof(drop), group::Val{false}) = Vector{ix_T}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{false}) = Vector{Union{NothingIndex, ix_T}}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{false}) = Vector{NothingIndex}()
empty_ix_vector(ix_T, nms::typeof(drop), group::Val{true}) = Vector{Vector{ix_T}}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{true}) = Vector{Vector{ix_T}}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{true}) = Vector{EmptyVector{ix_T, Vector}}()
