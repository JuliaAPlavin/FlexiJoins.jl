prepare_for_join(cache::Nothing, mode, X, cond::JoinCondition, multi) = fully_prepare(prepare_for_join(mode, X, cond, multi))
prepare_for_join(mode, X, cond::JoinCondition, multi) = prepare_for_join(mode, X, cond)


struct PreparedPartial
    prepared
    workspace_func
end

fully_prepare(pp::PreparedPartial) = (pp.prepared..., pp.workspace_func()...)
fully_prepare(x) = x


mutable struct JoinCache
    params::Union{Nothing, NamedTuple{(:mode, :cond, :multi, :Xid)}}
    prepared
    rlock::ReentrantLock
end

join_cache() = JoinCache(nothing, nothing, ReentrantLock())

function prepare_for_join(cache::JoinCache, mode, X, cond::JoinCondition, multi)
    lock(cache.rlock) do
        if isnothing(cache.prepared)
            @assert isnothing(cache.params)
            cache.params = (; mode, cond, multi, Xid=objectid(X))
            cache.prepared = prepare_for_join(mode, X, cond, multi)
        else
            @assert cache.params == (; mode, cond, multi, Xid=objectid(X))
        end
        return fully_prepare(cache.prepared)
    end
end
