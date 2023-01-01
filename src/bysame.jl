struct NotSame <: JoinCondition
    order_matters::Bool
end

not_same(; order_matters=true) = NotSame(order_matters)

normalize_arg(cond::NotSame, datas) = (@assert length(datas) == 2; cond)
swap_sides(cond::NotSame) = cond

supports_mode(mode::Mode.NestedLoop, cond::NotSame, datas) = first(datas) === last(datas)
supports_mode(mode::Mode.NestedLoopFast, cond::NotSame, datas) = first(datas) === last(datas)

is_match(cond::NotSame, ix_a, a, ix_b, b) = is_match_ix(cond, ix_a, ix_b)
is_match_ix(cond::NotSame, ix_a, ix_b) = cond.order_matters ? ix_a != ix_b : ix_a < ix_b
