module SkyCoordsExt
using SkyCoords
using FlexiJoins
using FlexiJoins: ByDistance, Mode

separation_to_chord(x) = 2 * sin(x / 2)
FlexiJoins.by_distance(func_L, func_R, dist::typeof(separation), maxpred::Base.Fix2) =
    by_distance(vec ∘ CartesianCoords{ICRSCoords}, FlexiJoins.NN.Distances.Euclidean(), maxpred.f(separation_to_chord(maxpred.x)))

end
