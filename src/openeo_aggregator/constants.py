JOB_OPTION_SPLIT_STRATEGY = "split_strategy"

JOB_OPTION_TILE_GRID = "tile_grid"

# Force a certain upstream back-end through job options
JOB_OPTION_FORCE_BACKEND = "federation:force-backend"
# TODO: remove support for legacy variant
JOB_OPTION_FORCE_BACKEND_LEGACY = "_agg_force_backend"


class CROSSBACKEND_GRAPH_SPLIT_METHOD:
    # Poor-man's StrEnum
    SIMPLE = "simple"
    DEEP = "deep"
