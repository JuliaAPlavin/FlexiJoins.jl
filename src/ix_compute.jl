function fill_ix_array!(IXs, datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::Nothing, cardinality)
	ix_seen_cnts = map(datas) do data
		map(Returns(0), data)
	end
	for ix_1 in keys(first(datas))
		IX_2 = findmatchix(cond, view(first(datas), ix_1), last(datas), last(multi))
		ix_seen_cnts[1][ix_1] += length(IX_2)
		@assert length(IX_2) ∈ cardinality[2]
		for ix_2 in IX_2
			ix_seen_cnts[2][ix_2] += 1
			push!(IXs, (ix_1, ix_2))
		end
	end
	add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches, groupby)
end

function fill_ix_array!(IXs, datas, cond, multi::Tuple{typeof(identity), Any}, nonmatches, groupby::StaticInt{1}, cardinality)
	ix_seen_cnts = map(datas) do data
		map(Returns(0), data)
	end
	for ix_1 in keys(first(datas))
		IX_2 = findmatchix(cond, view(first(datas), ix_1), last(datas), last(multi))
		ix_seen_cnts[1][ix_1] += length(IX_2)
		@assert length(IX_2) ∈ cardinality[2]
		for ix_2 in IX_2
			ix_seen_cnts[2][ix_2] += 1
		end
		isempty(IX_2) || push!(IXs, (ix_1, IX_2))
	end
	add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches, groupby)
end


function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(keep)}, groupby::Nothing)
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    for ix_2 in IX_2
        push!(IXs, (NothingIndex(), ix_2))
    end
    IX_1 = @p ix_seen_cnts[1] |> findall(==(0))
    for ix_1 in IX_1
        push!(IXs, (ix_1, NothingIndex()))
    end
    IXs
end

function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::Nothing)
    IX_1 = @p ix_seen_cnts[1] |> findall(==(0))
    for ix_1 in IX_1
        push!(IXs, (ix_1, NothingIndex()))
    end
    IXs
end

function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::Nothing)
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    for ix_2 in IX_2
        push!(IXs, (NothingIndex(), ix_2))
    end
    IXs
end

function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(keep)}, groupby::StaticInt{1})
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    push!(IXs, (NothingIndex(), IX_2))
    IX_1 = @p ix_seen_cnts[1] |> findall(==(0))
    for ix_1 in IX_1
        push!(IXs, (ix_1, []))
    end
    IXs
end

function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(keep), typeof(drop)}, groupby::StaticInt{1})
    IX_1 = @p ix_seen_cnts[1] |> findall(==(0))
    for ix_1 in IX_1
        push!(IXs, (ix_1, []))
    end
    IXs
end

function add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(keep)}, groupby::StaticInt{1})
    IX_2 = @p ix_seen_cnts[2] |> findall(==(0))
    push!(IXs, (NothingIndex(), IX_2))
    IXs
end

add_ix_nonmatches!(IXs, ix_seen_cnts, nonmatches::Tuple{typeof(drop), typeof(drop)}, groupby) = IXs

create_ix_array(datas, nonmatches, groupby::Nothing) = map(datas, reverse(nonmatches)) do X, nms
    empty_ix_vector(typeof(firstindex(X)), nms, Val(false))
end |> StructArray

create_ix_array(datas, nonmatches, groupby::StaticInt) = map(ntuple(identity, length(datas)), datas, reverse(nonmatches)) do i, X, nms
    empty_ix_vector(typeof(firstindex(X)), nms, Val(i != known(groupby)))
end |> StructArray

empty_ix_vector(ix_T, nms::typeof(drop), group::Val{false}) = Vector{ix_T}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{false}) = Vector{Union{NothingIndex, ix_T}}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{false}) = Vector{NothingIndex}()
empty_ix_vector(ix_T, nms::typeof(drop), group::Val{true}) = Vector{Vector{ix_T}}()
empty_ix_vector(ix_T, nms::typeof(keep), group::Val{true}) = Vector{Vector{ix_T}}()
empty_ix_vector(ix_T, nms::typeof(only), group::Val{true}) = Vector{EmptyVector{ix_T, Vector}}()
