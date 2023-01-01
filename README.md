# FlexiJoins.jl

`FlexiJoins.jl` is a fresh take on joining tabular or non-tabular datasets in Julia.

From simple joins by key, to asof joins, to merging catalogs of terrestrial or celestial coordinates – `FlexiJoins` supports any usecase.

Defining features of FlexiJoins that make it _flexible_:

- Wide range of join conditions: by key (so-called equi-join), by distance, by predicate, closest match (asof join)
- All kinds of joins, as in inner/left/right/outer
- Results can either be a flat list, or grouped by the left/right side
- Various dataset types transparently supported

With all these features, FlexiJoins is designed to be easy-to-use and fast:

- Uniform interface to all functionaly
- Performance close to other, less general, solutions: see [benchmarks](https://aplavin.github.io/FlexiJoins.jl/test/benchmarks.html)
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

Documentation with explanations and more examples is available as a [Pluto notebook](https://aplavin.github.io/FlexiJoins.jl/test/examples.html).
