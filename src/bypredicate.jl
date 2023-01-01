struct ByPred{TP, TL, TR} <: JoinCondition
    Lf::TL
    Rf::TR
    pred::TP
end

by_pred(Lf, pred, Rf) = ByPred(Lf, Rf, pred)
# by_pred(Lf, pred::typeof(isequal), Rf) = by_key((Lf, Rf))
# is_match(by::ByPred, a, b) = by.pred(a, b)

innerfunc(f::ComposedFunction) = innerfunc(f.inner)
innerfunc(f) = f
stripinner(f::ComposedFunction) = f.inner isa ComposedFunction ? f.outer ∘ stripinner(f.inner) : f.outer

function normalize_arg(cond::ByPred, datas::NamedTuple{NS}) where {NS}
    @assert length(NS) == 2
    # @assert innerfunc(cond.Lf) === Accessors.PropertyLens{NS[1]}()
    # @assert innerfunc(cond.Rf) === Accessors.PropertyLens{NS[2]}()
    # ByPred(stripinner(cond.Lf), stripinner(cond.Rf), cond.pred)
    cond
end


function optimize(which, datas, cond::ByPred{<:Union{typeof.((<, <=, ==, >=, >))...}}, multi)
    X = which(datas)
    (X, sortperm(X; by=which((cond.Lf, cond.Rf))))
end

findmatchix(cond::ByPred{typeof(<)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]

findmatchix(cond::ByPred{typeof(<=)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]

findmatchix(cond::ByPred{typeof(==)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

findmatchix(cond::ByPred{typeof(>=)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

findmatchix(cond::ByPred{typeof(>)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

function optimize(which, datas, cond::ByPred{typeof(∋)}, multi)
    X = which(datas)
    (X, sortperm(X; by=x -> which((cond.Lf, cond.Rf))(x)))
end

function findmatchix(cond::ByPred{typeof(∋)}, a, (B, perm)::Tuple, multi::typeof(identity))
    rng = cond.Lf(a)
    arr = mapview(i -> cond.Rf(B[i]), perm)
    if rng isa AbstractArray || rng isa AbstractSet
        @assert eltype(rng) == eltype(arr)
        @assert rng isa UnitRange
    end
    perm[searchsortedfirst(arr, minimum(rng)):searchsortedlast(arr, maximum(rng))]
end
