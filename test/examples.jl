### A Pluto.jl notebook ###
# v0.19.0

using Markdown
using InteractiveUtils

# ╔═╡ 24683ed0-b052-11ec-2ee1-dfbf11c71aa2
begin
	using Revise
	import Pkg
	eval(:(Pkg.develop(path="..")))
	Pkg.resolve()
	using FlexiJoins
end

# ╔═╡ a3d4e16a-3946-4d25-81ec-32db9d404c6a
using IntervalSets

# ╔═╡ 1b2cf280-332f-49f1-b8d7-27887e66cf98
using DataPipes

# ╔═╡ d4adeae4-ba96-46b0-acb8-4f2472c07e43
using Dictionaries

# ╔═╡ 651473bc-c206-4312-ab8b-0dfa8fda0672
using Distances

# ╔═╡ 66d5a7be-eace-4da3-8e30-a217d01849cd
using DisplayAs

# ╔═╡ 0a21983a-c6af-40e3-9898-091dee85dbf6
using StructArrays

# ╔═╡ 27cd8a65-e38d-4bf4-9013-a8054b116b00
using TypedTables

# ╔═╡ aafe09a8-480d-46dc-ac13-3adaddf94621
using PlutoUI

# ╔═╡ 095e9db4-8910-4ba9-b95a-518904d054c5
md"""
!!! warn "FlexiJoins"
	`FlexiJoins.jl` is a fresh take on joining tabular or non-tabular datasets in Julia.
	
	From simple joins by key, to asof joins, to merging catalogs of terrestrial or celestial coordinates --- `FlexiJoins` supports any usecase.
"""

# ╔═╡ f19a391a-b36a-429c-8e48-dc7b05a1f534
md"""
Defining features of `FlexiJoins` that make it _flexible_:
- Wide range of join conditions ([details below](#08b5e191-86cf-451d-87b3-8d2842953534)): \
  by key (so-called `equi-join`), by distance, by predicate, closest match (`asof join`)
- All kinds of joins, as in `inner/left/right/outer`: [see below](#37acfa2c-e002-4df4-b3e0-8a62ae62c69e)
- Results can either be a flat list, or grouped by the left/right side, [see below](#e6b9fde7-b8dc-4232-a436-be3ecdd4e113)
- Various dataset types transparently supported, [examples below](#6a1be3c5-dfb0-4fd7-94f1-25176cbd36a9)
"""

# ╔═╡ 95ee9ad5-0a7c-4955-912d-efc29202abf1
md"""
With all these features, `FlexiJoins` is designed to be easy-to-use and fast:
- Uniform interface to all functionaly
- Performance close to other, less general, solutions: see [benchmarks](https://aplavin.github.io/FlexiJoins.jl/test/benchmarks.html)
- Extensible in terms of both new join conditions and more specialized algorithms; see source code
"""

# ╔═╡ 5d25ada1-ca21-4776-a6fc-9926040d673b
md"""
Outside of `FlexiJoins.jl`, there seems to be only a single join implementation that works with various tables/arrays and doesn't require converting datasets to a specific type. The `SplitApplyCombine.jl` package implements inner and left joins, and return results as flat or grouped as well. It only performs key-based equijoins, however.
"""

# ╔═╡ 216b742f-6b4b-47d4-8cc0-6313b79437f6
md"""
# Basic usage
"""

# ╔═╡ 35bf52e6-2d11-49ed-a6cb-6a6d8af4cbef
md"""
Create two example tables:\
`objects` with `name` and `ref_time`,
"""

# ╔═╡ fcd06e43-ca78-4e92-a491-449cf1782b13
objects = [(name="A", ref_time=2), (name="B", ref_time=-5), (name="D", ref_time=1),  (name="E", ref_time=9)]

# ╔═╡ 4fad9980-e872-4068-9d2c-f32d7f896364
md"""
and `measurements` with `name` (same as in `objects`) and `time`.
"""

# ╔═╡ a47da704-89f1-418c-a96c-ebdec629d81b
measurements = [(name, time=t) for (name, cnt) in [("A", 4), ("B", 1), ("C", 3)] for t in cnt .* (2:(cnt+1))]

# ╔═╡ 64ef0e5a-fb92-4586-9605-11aaad5642e0
md"""
Now join these two tables in the simplest way by the `name` field:
"""

# ╔═╡ e77b8ad3-762c-4254-8dfe-4ee0d372e494
innerjoin((objects, measurements), by_key(:name)) |> DisplayAs.Unlimited

# ╔═╡ 7298365b-765a-4d02-af15-7a5d1f45fc12
md"""
!!! note
	Results are always a view of the original datasets. Use the `materialize_views` function to get materialized arrays, if actually needed: `innerjoin(...) |> materialize_views`.
"""

# ╔═╡ 7b095ba2-3b24-42ed-bf7b-e46a3637efc7
md"""
Each element in the resulting array is a pair of an `object` and a `measurement` that match according to the join condition.

It's often more intuitive and less error-prone to name both sides of the join:
"""

# ╔═╡ 5909fe8e-60e8-4502-ad75-f7a8d33fe7a3
innerjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ 54478c3a-73e5-4ce4-a5bf-4196b61346b7
md"""
!!! note
	It's useful to expand and inspect the join results in Pluto, both here and in examples below.
"""

# ╔═╡ 169df71b-a0e6-4f51-ba59-b042cff91e20
md"""
Here, the result entries are namedtuples. We'll follow this approach further.
"""

# ╔═╡ 4b818852-ad5f-4e1d-a3a0-4efcbd29f7f7
md"""
Naming join sides makes further processing cleaner, there is no confusion where each piece of data comes from:
"""

# ╔═╡ 31ba0ff7-0038-476c-adad-e95869873e5c
@p innerjoin((O=objects, M=measurements), by_key(:name)) |>
	map((; _.O.name, _.M.time, Δt=_.M.time - _.O.ref_time))

# ╔═╡ fcd5aea2-5eea-4f5c-a8c4-2ea67662995b
md"""
If the output is needed in the flat table form for some reason, it's easy to merge fields of both sides after joining:
"""

# ╔═╡ 1723c335-d31a-4295-b3ff-c2a336898596
@p innerjoin((O=objects, M=measurements), by_key(:name)) |>
	map(merge(_...))

# ╔═╡ 37acfa2c-e002-4df4-b3e0-8a62ae62c69e
md"""
# `inner/left/...` joins
"""

# ╔═╡ 4b7090c6-9eac-4c6e-89e4-3a1fb654e37f
md"""
`FlexiJoins.jl` provides convenience functions for the corresponding kinds of joins:
"""

# ╔═╡ 846bc59d-a8e5-406f-9a07-810a7465587b
innerjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ 5a9c36bf-d671-47c8-96f7-b60d6fe3f6d8
leftjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ f74afec8-bcd2-4413-a268-4f8b74e8689e
rightjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ c3aeb32c-626e-455a-9d95-e41d1cc0509b
outerjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ bea2727d-3976-4d58-b0b1-a87d130f8a9a
md"""
`nothing` in the results indicates that there are no matches on that side of the join.
"""

# ╔═╡ 2ebdf4cb-4d7e-4b3d-ba18-1ab5d037da9d
md"""
All these functions actually call the underlying `flexijoin(...)` function and assign its `nonmatches=` argument.\
This argument is an alternative way to specify how non-matches are treated:
"""

# ╔═╡ 4c45972f-83bd-4866-bce4-42d252c7353c
leftjoin((O=objects, M=measurements), by_key(:name); nonmatches=(O=keep, M=drop))  # same as leftjoin

# ╔═╡ e6b9fde7-b8dc-4232-a436-be3ecdd4e113
md"""
# Grouping results
"""

# ╔═╡ de4bfce5-4872-48e0-9f18-a5b7b93a090c
md"""
Join results can be returned as a flat list of matching pairs, or grouped by one of the join sides.

Flat list, as above:
"""

# ╔═╡ 1305276c-1dfe-4a4e-bf47-edfa2b4bc2a6
leftjoin((O=objects, M=measurements), by_key(:name))

# ╔═╡ dbd13148-ac1a-41d0-aded-99b5cf7c66c1
md"""
Grouping by the object (`:O`):
"""

# ╔═╡ c69e7cca-c038-4170-b2c6-c1dc57659532
leftjoin((O=objects, M=measurements), by_key(:name); groupby=:O)

# ╔═╡ 8ee40542-97ab-4981-8315-2ee02a7a5868
md"""
The latter, grouped, form is often more convenient for further processing:
"""

# ╔═╡ 5e677143-9f4d-4679-a87d-5513b35a3136
@p leftjoin((O=objects, M=measurements), by_key(:name); groupby=:O) |>
	map((; _.O, measurements_cnt=length(_.M)))

# ╔═╡ 08b5e191-86cf-451d-87b3-8d2842953534
md"""
# Join conditions
"""

# ╔═╡ 1fa5892d-7c2d-4b72-a6de-4dc9668a5f36
md"""
All the examples above only demonstrate the most basic join condition: key equality between elements of the first and the second dataset.

More advanced examples:
"""

# ╔═╡ d4ec4747-3c26-4fb6-a894-c50a997544a4
md"""
- Specify different keys for the two join sides (swap "B" and "C" in `measurement` names):
"""

# ╔═╡ 8f12b8c9-a07b-44a2-bc4d-340125098cb2
innerjoin((O=objects, M=measurements), by_key((O=:name, M=x -> replace(x.name, 'B' => 'C', 'C' => 'B'))))

# ╔═╡ 8de4a77f-49e7-4a63-9b8f-11baba776ca3
md"""
- Join by distance, selecting all measurement pairs separated by less than 3 units of time:
"""

# ╔═╡ 08984c01-0440-4636-82f6-cf28e0586edc
innerjoin((M1=measurements, M2=measurements), by_distance(:time, Euclidean(), <=(3)))

# ╔═╡ d81c61f1-9000-4f4f-bce9-d9ce85cdbfb4
md"""
`FlexiJoins` supports all distances in `Distances.jl`.
"""

# ╔═╡ 74ffc42f-4f27-4667-9517-41dde37ebd8f
md"""
- Join by both key and distance, limiting these matches to measurements of the same object:
"""

# ╔═╡ 61e49cea-2b7e-4fb5-89fb-2e147666bf83
innerjoin((M1=measurements, M2=measurements), by_key(:name) & by_distance(:time, Euclidean(), <=(3)))

# ╔═╡ e0aa09cc-67b4-4560-bc72-540e051d0ed2
md"""
- Join by a predicate, selecting only measurements later than the reference time for the object:
"""

# ╔═╡ 835f9d3a-2982-4c23-95dd-3460b5acf56a
innerjoin(
	(O=objects, M=measurements),
	by_key(:name) & by_pred(:ref_time, <, :time)
)

# ╔═╡ 5b430553-c81f-412c-bc2f-0a1165e6315f
md"""
- Join by a predicate, selecting only measurements less than 10 units of time later than the reference time for the object:
"""

# ╔═╡ c36a112d-c165-4e96-b6a7-ee5c3c636fff
innerjoin(
	(O=objects, M=measurements),
	by_key(:name) & by_pred(x -> x.ref_time..(x.ref_time + 10), ∋, :time)
)

# ╔═╡ 89f1ab33-b546-4f86-9dbc-d5a7a4af99be
md"""
Here, we create an interval for each object, and use the `∋` predicate condition.
"""

# ╔═╡ 59678194-c908-423e-8438-3857ecbc3d7e
md"""
- Select the closest match out of multiple: use the `multi=` argument, supported for `by_distance` and `by_pred` conditions when it makes sense:
"""

# ╔═╡ a39a323b-60ee-4d50-ad5f-f7541356f905
innerjoin(
	(O=objects, M=measurements),
	by_key(:name) & by_pred(:ref_time, <, :time);
	multi=(M=closest,)
)

# ╔═╡ 6a1be3c5-dfb0-4fd7-94f1-25176cbd36a9
md"""
# Supported dataset types
"""

# ╔═╡ a6545746-8509-46fb-876f-f1661c0aec7c
md"""
In addition to regular arrays, `FlexiJoins.jl` supports joining a wide range of other collection types.\
The only requirement is that they can be indexed and `view`ed as regular arrays.

Tables that don't fit this requirement should be converted first. Common conversion targers are a vector-of-namedtuples (row-oriented; use `Tables.rowtable()`) and a `StructArray` (column-oriented).
"""

# ╔═╡ 34a220c3-1937-4f57-b386-6eaeb9085e7c
md"""
Examples of supported types include:
"""

# ╔═╡ 94e2c182-e401-4042-a80d-cecb3f4dce4d
md"""
- `StructArray`s, that are basic tables with a column layout:
"""

# ╔═╡ 449d3cc5-e27b-474b-a4ed-7cf8f2af26e6
objs_SA = StructArray(objects)

# ╔═╡ d34a81d9-8e6e-4d0a-a55e-c50e4889df6f
innerjoin((O=objs_SA, M=measurements), by_key(:name))

# ╔═╡ 0675bb7d-94fa-46a1-a580-3357c9be7041
md"""
- `TypedTables`:
"""

# ╔═╡ 87204bf0-f62d-41a9-9b2f-fff5f0f7029f
objs_TT = Table(objects)

# ╔═╡ 6b68594b-2608-475f-bf29-b23aedd8a9c8
innerjoin((O=objs_TT, M=measurements), by_key(:name))

# ╔═╡ 280d4c51-27b9-4814-a65d-4378c13955fa
md"""
- `Dictionary`s:
"""

# ╔═╡ 186af722-b435-47d1-858f-252733af7203
meas_dict = dictionary('a':'h' .=> measurements)

# ╔═╡ 1c858fa5-d6b8-477f-bea9-276efc71050f
innerjoin((O=objs_SA, M=meas_dict), by_key(:name))

# ╔═╡ a847c938-c533-45ea-aa0b-f6c404d7fe61
md"""
# Join modes
"""

# ╔═╡ 00f6d1d7-3ee1-47b4-8dfe-37399613547f
md"""
`FlexiJoins.jl` support several modes of how joins are executed: they include nested loop join, sort join, hash join, and tree join.\
In regular use, the mode is selected automatically, based on what's supported by the specified join condition. For example, `by_key()` uses hash join by default, while `by_pred` uses sorting.

The naive nested loop join is never selected automatically, all joins use one of the optimized algorithms. Still, nested loops can be requested explicitly if ever needed:
"""

# ╔═╡ 4fda4c8e-8d7f-4341-a2e6-3db18292e50e
innerjoin((O=objects, M=measurements), by_key(:name); mode=FlexiJoins.Mode.NestedLoop())

# ╔═╡ 5f0b9787-259c-42ca-82b8-d393e470db8e
md"""
The results should not depend on the mode, which can be used to cross-check if there's a bug in an optimized join implementation.
"""

# ╔═╡ 6f844849-8e8d-4366-98a6-1d9203948eb0


# ╔═╡ c8a8c846-3c92-49bf-ada9-a7be80b61ee3


# ╔═╡ 21f8d454-9d89-4162-85ad-bf96cf1c2f94
PlutoUI.TableOfContents()

# ╔═╡ 00000000-0000-0000-0000-000000000001
PLUTO_PROJECT_TOML_CONTENTS = """
[deps]
DataPipes = "02685ad9-2d12-40c3-9f73-c6aeda6a7ff5"
Dictionaries = "85a47980-9c8c-11e8-2b9f-f7ca1fa99fb4"
DisplayAs = "0b91fe84-8a4c-11e9-3e1d-67c38462b6d6"
Distances = "b4f34e82-e78d-54a5-968a-f98e89d6e8f7"
FlexiJoins = "e37f2e79-19fa-4eb7-8510-b63b51fe0a37"
IntervalSets = "8197267c-284f-5f27-9208-e0e47529a953"
Pkg = "44cfe95a-1eb2-52ea-b672-e2afdf69b78f"
PlutoUI = "7f904dfe-b85e-4ff6-b463-dae2292396a8"
Revise = "295af30f-e4ad-537b-8983-00126c2a3abe"
StructArrays = "09ab397b-f2b6-538f-b94a-2f83cf4a842a"
TypedTables = "9d95f2ec-7b3d-5a63-8d20-e2491e220bb9"

[compat]
DataPipes = "~0.2.10"
Dictionaries = "~0.3.19"
DisplayAs = "~0.1.5"
Distances = "~0.10.7"
FlexiJoins = "~0.1.1"
IntervalSets = "~0.5.4"
PlutoUI = "~0.7.38"
Revise = "~3.3.3"
StructArrays = "~0.6.5"
TypedTables = "~1.4.0"
"""

# ╔═╡ 00000000-0000-0000-0000-000000000002
PLUTO_MANIFEST_TOML_CONTENTS = """
# This file is machine-generated - editing it directly is not advised

julia_version = "1.8.0-beta1"
manifest_format = "2.0"
project_hash = "3cbff40087c10b6f036030b94c32ffca7f24f955"

[[deps.AbstractPlutoDingetjes]]
deps = ["Pkg"]
git-tree-sha1 = "8eaf9f1b4921132a4cff3f36a1d9ba923b14a481"
uuid = "6e696c72-6542-2067-7265-42206c756150"
version = "1.1.4"

[[deps.Accessors]]
deps = ["Compat", "CompositionsBase", "ConstructionBase", "Future", "LinearAlgebra", "MacroTools", "Requires", "Test"]
git-tree-sha1 = "2bba2aa45df94e95b1a9c2405d7cfc3d60281db8"
uuid = "7d9f7c33-5ae7-4f3b-8dc6-eff91059b697"
version = "0.1.9"

[[deps.Adapt]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "af92965fb30777147966f58acb05da51c5616b5f"
uuid = "79e6a3ab-5dfb-504d-930d-738a2a938a0e"
version = "3.3.3"

[[deps.ArgTools]]
uuid = "0dad84c5-d112-42e6-8d28-ef12dabb789f"
version = "1.1.1"

[[deps.ArrayInterface]]
deps = ["Compat", "IfElse", "LinearAlgebra", "Requires", "SparseArrays", "Static"]
git-tree-sha1 = "c933ce606f6535a7c7b98e1d86d5d1014f730596"
uuid = "4fba245c-0d91-5ea0-9b3e-6abc04ee57a9"
version = "5.0.7"

[[deps.Artifacts]]
uuid = "56f22d72-fd6d-98f1-02f0-08ddc0907c33"

[[deps.Base64]]
uuid = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"

[[deps.CodeTracking]]
deps = ["InteractiveUtils", "UUIDs"]
git-tree-sha1 = "9fb640864691a0936f94f89150711c36072b0e8f"
uuid = "da1fd8a2-8d9e-5ec2-8556-3022fb5608a2"
version = "1.0.8"

[[deps.ColorTypes]]
deps = ["FixedPointNumbers", "Random"]
git-tree-sha1 = "024fe24d83e4a5bf5fc80501a314ce0d1aa35597"
uuid = "3da002f7-5984-5a60-b8a6-cbb66c0b333f"
version = "0.11.0"

[[deps.Compat]]
deps = ["Base64", "Dates", "DelimitedFiles", "Distributed", "InteractiveUtils", "LibGit2", "Libdl", "LinearAlgebra", "Markdown", "Mmap", "Pkg", "Printf", "REPL", "Random", "SHA", "Serialization", "SharedArrays", "Sockets", "SparseArrays", "Statistics", "Test", "UUIDs", "Unicode"]
git-tree-sha1 = "b153278a25dd42c65abbf4e62344f9d22e59191b"
uuid = "34da2185-b29b-5c13-b0c7-acf172513d20"
version = "3.43.0"

[[deps.CompilerSupportLibraries_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "e66e0078-7015-5450-92f7-15fbd957f2ae"
version = "0.5.0+0"

[[deps.CompositionsBase]]
git-tree-sha1 = "455419f7e328a1a2493cabc6428d79e951349769"
uuid = "a33af91c-f02d-484b-be07-31d278c5ca2b"
version = "0.1.1"

[[deps.ConstructionBase]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "f74e9d5388b8620b4cee35d4c5a618dd4dc547f4"
uuid = "187b0558-2788-49d3-abe0-74a17ed4e7c9"
version = "1.3.0"

[[deps.DataAPI]]
git-tree-sha1 = "cc70b17275652eb47bc9e5f81635981f13cea5c8"
uuid = "9a962f9c-6df0-11e9-0e5d-c546b8b5ee8a"
version = "1.9.0"

[[deps.DataPipes]]
deps = ["Accessors", "SplitApplyCombine"]
git-tree-sha1 = "058f621cced0c8d96f0aabd8deddb605acb33a86"
uuid = "02685ad9-2d12-40c3-9f73-c6aeda6a7ff5"
version = "0.2.10"

[[deps.DataValueInterfaces]]
git-tree-sha1 = "bfc1187b79289637fa0ef6d4436ebdfe6905cbd6"
uuid = "e2d170a0-9d28-54be-80f0-106bbe20a464"
version = "1.0.0"

[[deps.Dates]]
deps = ["Printf"]
uuid = "ade2ca70-3891-5945-98fb-dc099432e06a"

[[deps.DelimitedFiles]]
deps = ["Mmap"]
uuid = "8bb1440f-4735-579b-a4ab-409b98df4dab"

[[deps.Dictionaries]]
deps = ["Indexing", "Random"]
git-tree-sha1 = "0340cee29e3456a7de968736ceeb705d591875a2"
uuid = "85a47980-9c8c-11e8-2b9f-f7ca1fa99fb4"
version = "0.3.20"

[[deps.DisplayAs]]
git-tree-sha1 = "ac701a7b6da68758a0612ad9d81cd2ea897c95d4"
uuid = "0b91fe84-8a4c-11e9-3e1d-67c38462b6d6"
version = "0.1.5"

[[deps.Distances]]
deps = ["LinearAlgebra", "SparseArrays", "Statistics", "StatsAPI"]
git-tree-sha1 = "3258d0659f812acde79e8a74b11f17ac06d0ca04"
uuid = "b4f34e82-e78d-54a5-968a-f98e89d6e8f7"
version = "0.10.7"

[[deps.Distributed]]
deps = ["Random", "Serialization", "Sockets"]
uuid = "8ba89e20-285c-5b6f-9357-94700520ee1b"

[[deps.Downloads]]
deps = ["ArgTools", "FileWatching", "LibCURL", "NetworkOptions"]
uuid = "f43a241f-c20a-4ad4-852c-f6b1247861c6"
version = "1.6.0"

[[deps.EllipsisNotation]]
deps = ["ArrayInterface"]
git-tree-sha1 = "d064b0340db45d48893e7604ec95e7a2dc9da904"
uuid = "da5c29d0-fa7d-589e-88eb-ea29b0a81949"
version = "1.5.0"

[[deps.FileWatching]]
uuid = "7b1f6079-737a-58dc-b8bc-7a2ca5c1b5ee"

[[deps.FixedPointNumbers]]
deps = ["Statistics"]
git-tree-sha1 = "335bfdceacc84c5cdf16aadc768aa5ddfc5383cc"
uuid = "53c48c17-4a7d-5ca2-90c5-79b7896eea93"
version = "0.8.4"

[[deps.FlexiJoins]]
deps = ["Accessors", "DataPipes", "Indexing", "IntervalSets", "NearestNeighbors", "SplitApplyCombine", "Static", "StructArrays"]
path = "../../home/aplavin/.julia/dev/FlexiJoins"
uuid = "e37f2e79-19fa-4eb7-8510-b63b51fe0a37"
version = "0.1.1"

[[deps.Future]]
deps = ["Random"]
uuid = "9fa8497b-333b-5362-9e8d-4d0656e87820"

[[deps.Hyperscript]]
deps = ["Test"]
git-tree-sha1 = "8d511d5b81240fc8e6802386302675bdf47737b9"
uuid = "47d2ed2b-36de-50cf-bf87-49c2cf4b8b91"
version = "0.0.4"

[[deps.HypertextLiteral]]
git-tree-sha1 = "2b078b5a615c6c0396c77810d92ee8c6f470d238"
uuid = "ac1192a8-f4b3-4bfe-ba22-af5b92cd3ab2"
version = "0.9.3"

[[deps.IOCapture]]
deps = ["Logging", "Random"]
git-tree-sha1 = "f7be53659ab06ddc986428d3a9dcc95f6fa6705a"
uuid = "b5f81e59-6552-4d32-b1f0-c071b021bf89"
version = "0.2.2"

[[deps.IfElse]]
git-tree-sha1 = "debdd00ffef04665ccbb3e150747a77560e8fad1"
uuid = "615f187c-cbe4-4ef1-ba3b-2fcf58d6d173"
version = "0.1.1"

[[deps.Indexing]]
git-tree-sha1 = "ce1566720fd6b19ff3411404d4b977acd4814f9f"
uuid = "313cdc1a-70c2-5d6a-ae34-0150d3930a38"
version = "1.1.1"

[[deps.InteractiveUtils]]
deps = ["Markdown"]
uuid = "b77e0a4c-d291-57a0-90e8-8db25a27a240"

[[deps.IntervalSets]]
deps = ["Dates", "EllipsisNotation", "Statistics"]
git-tree-sha1 = "bcf640979ee55b652f3b01650444eb7bbe3ea837"
uuid = "8197267c-284f-5f27-9208-e0e47529a953"
version = "0.5.4"

[[deps.IteratorInterfaceExtensions]]
git-tree-sha1 = "a3f24677c21f5bbe9d2a714f95dcd58337fb2856"
uuid = "82899510-4779-5014-852e-03e436cf321d"
version = "1.0.0"

[[deps.JSON]]
deps = ["Dates", "Mmap", "Parsers", "Unicode"]
git-tree-sha1 = "3c837543ddb02250ef42f4738347454f95079d4e"
uuid = "682c06a0-de6a-54ab-a142-c8b1cf79cde6"
version = "0.21.3"

[[deps.JuliaInterpreter]]
deps = ["CodeTracking", "InteractiveUtils", "Random", "UUIDs"]
git-tree-sha1 = "52617c41d2761cc05ed81fe779804d3b7f14fff7"
uuid = "aa1ae85d-cabe-5617-a682-6adf51b2e16a"
version = "0.9.13"

[[deps.LibCURL]]
deps = ["LibCURL_jll", "MozillaCACerts_jll"]
uuid = "b27032c2-a3e7-50c8-80cd-2d36dbcbfd21"
version = "0.6.3"

[[deps.LibCURL_jll]]
deps = ["Artifacts", "LibSSH2_jll", "Libdl", "MbedTLS_jll", "Zlib_jll", "nghttp2_jll"]
uuid = "deac9b47-8bc7-5906-a0fe-35ac56dc84c0"
version = "7.81.0+0"

[[deps.LibGit2]]
deps = ["Base64", "NetworkOptions", "Printf", "SHA"]
uuid = "76f85450-5226-5b5a-8eaa-529ad045b433"

[[deps.LibSSH2_jll]]
deps = ["Artifacts", "Libdl", "MbedTLS_jll"]
uuid = "29816b5a-b9ab-546f-933c-edad1886dfa8"
version = "1.10.2+0"

[[deps.Libdl]]
uuid = "8f399da3-3557-5675-b5ff-fb832c97cbdb"

[[deps.LinearAlgebra]]
deps = ["Libdl", "libblastrampoline_jll"]
uuid = "37e2e46d-f89d-539d-b4ee-838fcccc9c8e"

[[deps.Logging]]
uuid = "56ddb016-857b-54e1-b83d-db4d58db5568"

[[deps.LoweredCodeUtils]]
deps = ["JuliaInterpreter"]
git-tree-sha1 = "6b0440822974cab904c8b14d79743565140567f6"
uuid = "6f1432cf-f94c-5a45-995e-cdbf5db27b0b"
version = "2.2.1"

[[deps.MacroTools]]
deps = ["Markdown", "Random"]
git-tree-sha1 = "3d3e902b31198a27340d0bf00d6ac452866021cf"
uuid = "1914dd2f-81c6-5fcd-8719-6d5c9610ff09"
version = "0.5.9"

[[deps.Markdown]]
deps = ["Base64"]
uuid = "d6f4376e-aef5-505a-96c1-9c027394607a"

[[deps.MbedTLS_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "c8ffd9c3-330d-5841-b78e-0817d7145fa1"
version = "2.28.0+0"

[[deps.Mmap]]
uuid = "a63ad114-7e13-5084-954f-fe012c677804"

[[deps.MozillaCACerts_jll]]
uuid = "14a3606d-f60d-562e-9121-12d972cd8159"
version = "2022.2.1"

[[deps.NearestNeighbors]]
deps = ["Distances", "StaticArrays"]
git-tree-sha1 = "ded92de95031d4a8c61dfb6ba9adb6f1d8016ddd"
uuid = "b8a86587-4115-5ab1-83bc-aa920d37bbce"
version = "0.4.10"

[[deps.NetworkOptions]]
uuid = "ca575930-c2e3-43a9-ace4-1e988b2c1908"
version = "1.2.0"

[[deps.OpenBLAS_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "Libdl"]
uuid = "4536629a-c528-5b80-bd46-f80d51c5b363"
version = "0.3.17+2"

[[deps.OrderedCollections]]
git-tree-sha1 = "85f8e6578bf1f9ee0d11e7bb1b1456435479d47c"
uuid = "bac558e1-5e72-5ebc-8fee-abe8a469f55d"
version = "1.4.1"

[[deps.Parsers]]
deps = ["Dates"]
git-tree-sha1 = "621f4f3b4977325b9128d5fae7a8b4829a0c2222"
uuid = "69de0a69-1ddd-5017-9359-2bf0b02dc9f0"
version = "2.2.4"

[[deps.Pkg]]
deps = ["Artifacts", "Dates", "Downloads", "LibGit2", "Libdl", "Logging", "Markdown", "Printf", "REPL", "Random", "SHA", "Serialization", "TOML", "Tar", "UUIDs", "p7zip_jll"]
uuid = "44cfe95a-1eb2-52ea-b672-e2afdf69b78f"
version = "1.8.0"

[[deps.PlutoUI]]
deps = ["AbstractPlutoDingetjes", "Base64", "ColorTypes", "Dates", "Hyperscript", "HypertextLiteral", "IOCapture", "InteractiveUtils", "JSON", "Logging", "Markdown", "Random", "Reexport", "UUIDs"]
git-tree-sha1 = "670e559e5c8e191ded66fa9ea89c97f10376bb4c"
uuid = "7f904dfe-b85e-4ff6-b463-dae2292396a8"
version = "0.7.38"

[[deps.Printf]]
deps = ["Unicode"]
uuid = "de0858da-6303-5e67-8744-51eddeeeb8d7"

[[deps.REPL]]
deps = ["InteractiveUtils", "Markdown", "Sockets", "Unicode"]
uuid = "3fa0cd96-eef1-5676-8a61-b3b8758bbffb"

[[deps.Random]]
deps = ["SHA", "Serialization"]
uuid = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"

[[deps.Reexport]]
git-tree-sha1 = "45e428421666073eab6f2da5c9d310d99bb12f9b"
uuid = "189a3867-3050-52da-a836-e630ba90ab69"
version = "1.2.2"

[[deps.Requires]]
deps = ["UUIDs"]
git-tree-sha1 = "838a3a4188e2ded87a4f9f184b4b0d78a1e91cb7"
uuid = "ae029012-a4dd-5104-9daa-d747884805df"
version = "1.3.0"

[[deps.Revise]]
deps = ["CodeTracking", "Distributed", "FileWatching", "JuliaInterpreter", "LibGit2", "LoweredCodeUtils", "OrderedCollections", "Pkg", "REPL", "Requires", "UUIDs", "Unicode"]
git-tree-sha1 = "4d4239e93531ac3e7ca7e339f15978d0b5149d03"
uuid = "295af30f-e4ad-537b-8983-00126c2a3abe"
version = "3.3.3"

[[deps.SHA]]
uuid = "ea8e919c-243c-51af-8825-aaa63cd721ce"
version = "0.7.0"

[[deps.Serialization]]
uuid = "9e88b42a-f829-5b0c-bbe9-9e923198166b"

[[deps.SharedArrays]]
deps = ["Distributed", "Mmap", "Random", "Serialization"]
uuid = "1a1011a3-84de-559e-8e89-a11a2f7dc383"

[[deps.Sockets]]
uuid = "6462fe0b-24de-5631-8697-dd941f90decc"

[[deps.SparseArrays]]
deps = ["LinearAlgebra", "Random"]
uuid = "2f01184e-e22b-5df5-ae63-d93ebab69eaf"

[[deps.SplitApplyCombine]]
deps = ["Dictionaries", "Indexing"]
git-tree-sha1 = "35efd62f6f8d9142052d9c7a84e35cd1f9d2db29"
uuid = "03a91e81-4c3e-53e1-a0a4-9c0c8f19dd66"
version = "1.2.1"

[[deps.Static]]
deps = ["IfElse"]
git-tree-sha1 = "87e9954dfa33fd145694e42337bdd3d5b07021a6"
uuid = "aedffcd0-7271-4cad-89d0-dc628f76c6d3"
version = "0.6.0"

[[deps.StaticArrays]]
deps = ["LinearAlgebra", "Random", "Statistics"]
git-tree-sha1 = "4f6ec5d99a28e1a749559ef7dd518663c5eca3d5"
uuid = "90137ffa-7385-5640-81b9-e52037218182"
version = "1.4.3"

[[deps.Statistics]]
deps = ["LinearAlgebra", "SparseArrays"]
uuid = "10745b16-79ce-11e8-11f9-7d13ad32a3b2"

[[deps.StatsAPI]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "8d7530a38dbd2c397be7ddd01a424e4f411dcc41"
uuid = "82ae8749-77ed-4fe6-ae5f-f523153014b0"
version = "1.2.2"

[[deps.StructArrays]]
deps = ["Adapt", "DataAPI", "StaticArrays", "Tables"]
git-tree-sha1 = "57617b34fa34f91d536eb265df67c2d4519b8b98"
uuid = "09ab397b-f2b6-538f-b94a-2f83cf4a842a"
version = "0.6.5"

[[deps.TOML]]
deps = ["Dates"]
uuid = "fa267f1f-6049-4f14-aa54-33bafae1ed76"
version = "1.0.0"

[[deps.TableTraits]]
deps = ["IteratorInterfaceExtensions"]
git-tree-sha1 = "c06b2f539df1c6efa794486abfb6ed2022561a39"
uuid = "3783bdb8-4a98-5b6b-af9a-565f29a5fe9c"
version = "1.0.1"

[[deps.Tables]]
deps = ["DataAPI", "DataValueInterfaces", "IteratorInterfaceExtensions", "LinearAlgebra", "OrderedCollections", "TableTraits", "Test"]
git-tree-sha1 = "5ce79ce186cc678bbb5c5681ca3379d1ddae11a1"
uuid = "bd369af6-aec1-5ad0-b16a-f7cc5008161c"
version = "1.7.0"

[[deps.Tar]]
deps = ["ArgTools", "SHA"]
uuid = "a4e569a6-e804-4fa4-b0f3-eef7a1d5b13e"
version = "1.10.0"

[[deps.Test]]
deps = ["InteractiveUtils", "Logging", "Random", "Serialization"]
uuid = "8dfed614-e22c-5e08-85e1-65c5234f0b40"

[[deps.TypedTables]]
deps = ["Adapt", "Dictionaries", "Indexing", "SplitApplyCombine", "Tables", "Unicode"]
git-tree-sha1 = "f91a10d0132310a31bc4f8d0d29ce052536bd7d7"
uuid = "9d95f2ec-7b3d-5a63-8d20-e2491e220bb9"
version = "1.4.0"

[[deps.UUIDs]]
deps = ["Random", "SHA"]
uuid = "cf7118a7-6976-5b1a-9a39-7adc72f591a4"

[[deps.Unicode]]
uuid = "4ec0a83e-493e-50e2-b9ac-8f72acf5a8f5"

[[deps.Zlib_jll]]
deps = ["Libdl"]
uuid = "83775a58-1f1d-513f-b197-d71354ab007a"
version = "1.2.12+1"

[[deps.libblastrampoline_jll]]
deps = ["Artifacts", "Libdl", "OpenBLAS_jll"]
uuid = "8e850b90-86db-534c-a0d3-1478176c7d93"
version = "5.0.1+0"

[[deps.nghttp2_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850ede-7688-5339-a07c-302acd2aaf8d"
version = "1.41.0+1"

[[deps.p7zip_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "3f19e933-33d8-53b3-aaab-bd5110c3b7a0"
version = "16.2.1+1"
"""

# ╔═╡ Cell order:
# ╠═095e9db4-8910-4ba9-b95a-518904d054c5
# ╟─f19a391a-b36a-429c-8e48-dc7b05a1f534
# ╟─95ee9ad5-0a7c-4955-912d-efc29202abf1
# ╟─5d25ada1-ca21-4776-a6fc-9926040d673b
# ╟─216b742f-6b4b-47d4-8cc0-6313b79437f6
# ╟─35bf52e6-2d11-49ed-a6cb-6a6d8af4cbef
# ╠═fcd06e43-ca78-4e92-a491-449cf1782b13
# ╟─4fad9980-e872-4068-9d2c-f32d7f896364
# ╠═a47da704-89f1-418c-a96c-ebdec629d81b
# ╟─64ef0e5a-fb92-4586-9605-11aaad5642e0
# ╠═e77b8ad3-762c-4254-8dfe-4ee0d372e494
# ╟─7298365b-765a-4d02-af15-7a5d1f45fc12
# ╟─7b095ba2-3b24-42ed-bf7b-e46a3637efc7
# ╠═5909fe8e-60e8-4502-ad75-f7a8d33fe7a3
# ╟─54478c3a-73e5-4ce4-a5bf-4196b61346b7
# ╟─169df71b-a0e6-4f51-ba59-b042cff91e20
# ╟─4b818852-ad5f-4e1d-a3a0-4efcbd29f7f7
# ╠═31ba0ff7-0038-476c-adad-e95869873e5c
# ╟─fcd5aea2-5eea-4f5c-a8c4-2ea67662995b
# ╠═1723c335-d31a-4295-b3ff-c2a336898596
# ╟─37acfa2c-e002-4df4-b3e0-8a62ae62c69e
# ╟─4b7090c6-9eac-4c6e-89e4-3a1fb654e37f
# ╠═846bc59d-a8e5-406f-9a07-810a7465587b
# ╠═5a9c36bf-d671-47c8-96f7-b60d6fe3f6d8
# ╠═f74afec8-bcd2-4413-a268-4f8b74e8689e
# ╠═c3aeb32c-626e-455a-9d95-e41d1cc0509b
# ╟─bea2727d-3976-4d58-b0b1-a87d130f8a9a
# ╟─2ebdf4cb-4d7e-4b3d-ba18-1ab5d037da9d
# ╠═4c45972f-83bd-4866-bce4-42d252c7353c
# ╟─e6b9fde7-b8dc-4232-a436-be3ecdd4e113
# ╟─de4bfce5-4872-48e0-9f18-a5b7b93a090c
# ╠═1305276c-1dfe-4a4e-bf47-edfa2b4bc2a6
# ╟─dbd13148-ac1a-41d0-aded-99b5cf7c66c1
# ╠═c69e7cca-c038-4170-b2c6-c1dc57659532
# ╟─8ee40542-97ab-4981-8315-2ee02a7a5868
# ╠═5e677143-9f4d-4679-a87d-5513b35a3136
# ╟─08b5e191-86cf-451d-87b3-8d2842953534
# ╟─1fa5892d-7c2d-4b72-a6de-4dc9668a5f36
# ╟─d4ec4747-3c26-4fb6-a894-c50a997544a4
# ╠═8f12b8c9-a07b-44a2-bc4d-340125098cb2
# ╟─8de4a77f-49e7-4a63-9b8f-11baba776ca3
# ╠═08984c01-0440-4636-82f6-cf28e0586edc
# ╟─d81c61f1-9000-4f4f-bce9-d9ce85cdbfb4
# ╟─74ffc42f-4f27-4667-9517-41dde37ebd8f
# ╠═61e49cea-2b7e-4fb5-89fb-2e147666bf83
# ╟─e0aa09cc-67b4-4560-bc72-540e051d0ed2
# ╠═835f9d3a-2982-4c23-95dd-3460b5acf56a
# ╟─5b430553-c81f-412c-bc2f-0a1165e6315f
# ╠═c36a112d-c165-4e96-b6a7-ee5c3c636fff
# ╟─89f1ab33-b546-4f86-9dbc-d5a7a4af99be
# ╟─59678194-c908-423e-8438-3857ecbc3d7e
# ╠═a39a323b-60ee-4d50-ad5f-f7541356f905
# ╟─6a1be3c5-dfb0-4fd7-94f1-25176cbd36a9
# ╟─a6545746-8509-46fb-876f-f1661c0aec7c
# ╟─34a220c3-1937-4f57-b386-6eaeb9085e7c
# ╟─94e2c182-e401-4042-a80d-cecb3f4dce4d
# ╠═449d3cc5-e27b-474b-a4ed-7cf8f2af26e6
# ╠═d34a81d9-8e6e-4d0a-a55e-c50e4889df6f
# ╟─0675bb7d-94fa-46a1-a580-3357c9be7041
# ╠═87204bf0-f62d-41a9-9b2f-fff5f0f7029f
# ╠═6b68594b-2608-475f-bf29-b23aedd8a9c8
# ╟─280d4c51-27b9-4814-a65d-4378c13955fa
# ╠═186af722-b435-47d1-858f-252733af7203
# ╠═1c858fa5-d6b8-477f-bea9-276efc71050f
# ╟─a847c938-c533-45ea-aa0b-f6c404d7fe61
# ╟─00f6d1d7-3ee1-47b4-8dfe-37399613547f
# ╠═4fda4c8e-8d7f-4341-a2e6-3db18292e50e
# ╟─5f0b9787-259c-42ca-82b8-d393e470db8e
# ╠═6f844849-8e8d-4366-98a6-1d9203948eb0
# ╠═c8a8c846-3c92-49bf-ada9-a7be80b61ee3
# ╠═24683ed0-b052-11ec-2ee1-dfbf11c71aa2
# ╠═a3d4e16a-3946-4d25-81ec-32db9d404c6a
# ╠═1b2cf280-332f-49f1-b8d7-27887e66cf98
# ╠═d4adeae4-ba96-46b0-acb8-4f2472c07e43
# ╠═651473bc-c206-4312-ab8b-0dfa8fda0672
# ╠═66d5a7be-eace-4da3-8e30-a217d01849cd
# ╠═0a21983a-c6af-40e3-9898-091dee85dbf6
# ╠═27cd8a65-e38d-4bf4-9013-a8054b116b00
# ╠═aafe09a8-480d-46dc-ac13-3adaddf94621
# ╠═21f8d454-9d89-4162-85ad-bf96cf1c2f94
# ╟─00000000-0000-0000-0000-000000000001
# ╟─00000000-0000-0000-0000-000000000002
