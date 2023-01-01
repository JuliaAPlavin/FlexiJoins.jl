prepare_for_join(cache::Nothing, mode, X, cond::JoinCondition, multi) = prepare_for_join(mode, X, cond, multi)
prepare_for_join(mode, X, cond::JoinCondition, multi) = prepare_for_join(mode, X, cond)


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
        return cache.prepared
    end
end
