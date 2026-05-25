#pragma once

#include <components/logical_plan/node.hpp>
#include <set>
#include <string>

namespace components::logical_plan {

    std::set<std::string> collect_referenced_columns(const expressions::expression_ptr& expr);

    class plan_optimizer_t {
    public:
        node_ptr optimize(node_ptr plan);
    private:
        node_ptr pushdown_filter(node_ptr node);
    };

} // namespace components::logical_plan
