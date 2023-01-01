# findmatchix(cond::JoinCondition, a, B, multi::typeof(identity)) = findall(is_match(cond, a), B)
# findmatchix(cond::JoinCondition, a, B, multi::typeof(first)) = let
#     ix = findfirst(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end
# findmatchix(cond::JoinCondition, a, B, multi::typeof(last)) = let
#     ix = findlast(is_match(cond, a), B)
#     isnothing(ix) ? [] : [ix]
# end
