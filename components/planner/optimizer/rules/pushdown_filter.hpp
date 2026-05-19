#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Pushes node_match_t below identity SELECT / sort / projection-only group /
    // appropriate side of JOIN. Operates on symbolic column names — safe to run
    // BEFORE schema validation.
    logical_plan::node_ptr pushdown_filter(logical_plan::node_ptr node);

} // namespace components::planner::optimizer
