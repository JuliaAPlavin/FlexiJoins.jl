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

normalize_arg(cond::ByPred, datas) = (@assert length(datas) == 2; cond)

supports_mode(::Mode.SortChain, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, datas) = true


function optimize(::Mode.Sort, which, datas, cond::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, multi)
    X = which(datas)
    (X, sortperm(X; by=which((cond.Lf, cond.Rf))))
end

findmatchix(::Mode.Sort, cond::ByPred{typeof(<)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]

findmatchix(::Mode.Sort, cond::ByPred{typeof(<=)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]

findmatchix(::Mode.Sort, cond::ByPred{typeof(==)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

findmatchix(::Mode.Sort, cond::ByPred{typeof(>=)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

findmatchix(::Mode.Sort, cond::ByPred{typeof(>)}, a, (B, perm)::Tuple, multi::typeof(identity)) =
    perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

function findmatchix(::Mode.Sort, cond::ByPred{typeof(∋)}, a, (B, perm)::Tuple, multi::typeof(identity))
    rng = cond.Lf(a)
    @assert rng isa Interval
    arr = mapview(i -> cond.Rf(B[i]), perm)
    perm[searchsortedfirst(arr, minimum(rng)):searchsortedlast(arr, maximum(rng))]
end
