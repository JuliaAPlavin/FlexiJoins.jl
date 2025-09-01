# FlexiJoins.jl

`FlexiJoins.jl` is a fresh take on joining tabular or non-tabular datasets in Julia.

From simple joins by key, to asof joins, to merging catalogs of terrestrial or celestial coordinates – `FlexiJoins` supports any usecase.

Defining features of FlexiJoins that make it _flexible_:

- Wide range of join conditions:
    - by key, so-called equi-join
	- by distance
	- by a comparison predicate: one of `<, <=, ==, >=, >`
	- all matches or only the closest match
	- by an interval predicate: one of `∈, ⊆, ⊊, ⊋, ⊇, !isdisjoint`
	- combinations of the above
- All kinds of joins, as in inner/left/right/outer
- Results can either be a flat list, or grouped by the left/right side
- Lots of dataset types transparently supported: various arrays, dictionaries, tables
- And more! See [examples](https://aplavin.github.io/FlexiJoins.jl/notebooks/examples.html).

With all these features, FlexiJoins is designed to be easy-to-use and fast:

- Uniform interface to all functionaly
- Performance close to other, less general, solutions: see [benchmarks](https://aplavin.github.io/FlexiJoins.jl/notebooks/benchmarks.html) comparing with `SplitApplyCombine.jl` and `DataFrames.jl`
- Extensible in terms of both new join conditions and more specialized algorithms

# Usage

Examples that showcase main features:

```julia
innerjoin((objects, measurements), by_key(:name))

leftjoin((O=objects, M=measurements), by_key(:name); groupby=:O)

innerjoin((M1=measurements, M2=measurements), by_key(:name) & by_distance(:time, Euclidean(), <=(3)))

innerjoin(
	(O=objects, M=measurements),
	by_key(:name) & by_pred(:ref_time, <, :time);
	multi=(M=closest,)
)
```

See [documentation](https://aplavin.github.io/FlexiJoins.jl/notebooks/examples.html) for more details and examples.

# Integrations

FlexiJoins is extensible: there are integrations with a number of packages, providing more specialized join conditions where it makes sense.
Featured integrations include:
- by-uncertainty joining with [Uncertain.jl](https://github.com/JuliaAPlavin/Uncertain.jl) – effectively, a distance join with different thresholds for each element
- spatial joins with [GeometryOps.jl](https://github.com/JuliaGeo/GeometryOps.jl)
- astronomical catalogs matching with [SkyCoords.jl](https://github.com/JuliaAstro/SkyCoords.jl) – see [quickstart and examples](https://aplavin.github.io/FlexiJoins.jl/notebooks/skycoords.html)

# Contributing

Please report bugs/issues on github, and direct usage questions to the [discourse topic](https://discourse.julialang.org/t/ann-flexijoins-jl-fresh-take-on-joining-all-kinds-of-datasets/79655).
