#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/pushdown_filter.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const catalog::catalog* /*catalog*/,
                                    logical_plan::parameter_node_t* parameters,
                                    const optimizer_options& options) {
        if (!node) {
            return nullptr;
        }

        // Constant folding: resolve arithmetic on parameters at plan time
        if (options.fold_constants && parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }

        // Filter pushdown: relocate node_match_t closer to its data source.
        // Safe to run before validate_schema — operates on symbolic column names.
        if (options.pushdown_filter) {
            node = optimizer::pushdown_filter(node);
        }

        return node;
    }

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const catalog::catalog* catalog,
                                    logical_plan::parameter_node_t* parameters) {
        return optimize(resource, std::move(node), catalog, parameters, optimizer_options{});
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* /*resource*/,
                                                  logical_plan::node_ptr node,
                                                  const catalog::catalog* catalog) {
        if (!node) {
            return nullptr;
        }

        // Column pruning: annotate aggregate nodes with the set of columns each one
        // actually needs to read from its source. Requires resolved paths, so must run
        // after validate_schema.
        optimizer::prune_columns(node, catalog);

        return node;
    }

} // namespace components::planner
