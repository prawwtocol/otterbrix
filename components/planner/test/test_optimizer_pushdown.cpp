#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/optimizer.hpp>
#include <components/tests/generaty.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

namespace {
    collection_full_name_t coll() { return {database_name_t("db"), collection_name_t("t")}; }

    node_data_ptr make_data_with_cols(std::pmr::memory_resource* r) {
        std::pmr::vector<components::types::complex_logical_type> types(r);
        types.emplace_back(components::types::logical_type::BIGINT, "a");
        types.emplace_back(components::types::logical_type::BIGINT, "b");
        auto chunk = gen_data_chunk(1, 0, types, r);
        return make_node_raw_data(r, std::move(chunk));
    }
} // namespace

TEST_CASE("planner::optimize pushes filter under identity select") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data_with_cols(&resource);

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    auto params = make_parameter_node(&resource);
    node_ptr out = components::planner::optimize(&resource,
                                                 boost::static_pointer_cast<node_t>(outer),
                                                 nullptr,
                                                 params.get());

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}
