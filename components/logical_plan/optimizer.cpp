#include "optimizer.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_select.hpp>

namespace components::logical_plan {

    namespace {

        // Predicate may move below node_select_t only if every filter column is a visible
        // select output with a get_field that does not rename (same output and input name).
        bool filter_supported_through_identity_select(const node_select_t& sel,
                                                      const std::set<std::string>& filter_cols,
                                                      const std::set<std::string>& input_cols) {
            const auto& exprs = sel.expressions();
            const size_t hidden = sel.internal_aggregate_count;
            if (exprs.size() < hidden) {
                return false;
            }
            const size_t visible = exprs.size() - hidden;

            for (const auto& col : filter_cols) {
                if (input_cols.find(col) == input_cols.end()) {
                    return false;
                }
                bool ok_for_col = false;
                for (size_t i = 0; i < visible; ++i) {
                    const auto& expr = exprs[i];
                    if (expr->group() != expressions::expression_group::scalar) {
                        return false;
                    }
                    auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                    if (sc->type() != expressions::scalar_type::get_field) {
                        continue;
                    }
                    const std::string out = sc->key().as_string();
                    if (out != col) {
                        continue;
                    }
                    if (sc->params().empty()) {
                        ok_for_col = true;
                        break;
                    }
                    if (sc->params().size() == 1 &&
                        std::holds_alternative<expressions::key_t>(sc->params().front())) {
                        const auto& in_key = std::get<expressions::key_t>(sc->params().front());
                        if (in_key.as_string() == col) {
                            ok_for_col = true;
                            break;
                        }
                    }
                }
                if (!ok_for_col) {
                    return false;
                }
            }
            return true;
        }

        void extract_from_param(const expressions::param_storage& param,
                                std::set<std::string>& result) {
            // из параметра выражения достаёт либо ключ столбца, либо рекурсивно обходит вложенное выражение.
            if (std::holds_alternative<expressions::key_t>(param)) {
                result.insert(std::get<expressions::key_t>(param).as_string());
            } else if (std::holds_alternative<expressions::expression_ptr>(param)) {
                auto cols = collect_referenced_columns(std::get<expressions::expression_ptr>(param));
                result.insert(cols.begin(), cols.end());
            }
            // parameter_id_t — literal constant, no column refs
        }

        void collect_subtree_columns(const node_ptr& node, std::set<std::string>& cols) {
            // для узла data собирает алиасы столбцов из чанка; для остальных спускается к детям (нужно понять, какие имена доступны под веткой join)
            if (!node) return;
            if (node->type() == node_type::data_t) {
                auto* data = static_cast<node_data_t*>(node.get());
                auto types = data->data_chunk().types();
                for (size_t i = 0; i < types.size(); ++i) {
                    if (!types[i].alias().empty())
                        cols.insert(types[i].alias());
                }
                return;
            }
            for (const auto& child : node->children()) {
                collect_subtree_columns(child, cols);
            }
        }

    } // anonymous namespace

    std::set<std::string> collect_referenced_columns(const expressions::expression_ptr& expr) {
        std::set<std::string> result;
        if (!expr) return result;

        switch (expr->group()) {
            // обход по типам выражений
            case expressions::expression_group::compare: {
                auto* cmp = static_cast<expressions::compare_expression_t*>(expr.get());
                if (expressions::is_union_compare_condition(cmp->type())) {
                    for (const auto& child : cmp->children()) {
                        auto cols = collect_referenced_columns(child);
                        result.insert(cols.begin(), cols.end());
                    }
                } else {
                    extract_from_param(cmp->left(), result);
                    extract_from_param(cmp->right(), result);
                }
                break;
            }
            case expressions::expression_group::scalar: {
                auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                for (const auto& param : sc->params()) {
                    extract_from_param(param, result);
                }
                break;
            }
            case expressions::expression_group::aggregate: {
                auto* agg = static_cast<expressions::aggregate_expression_t*>(expr.get());
                for (const auto& param : agg->params()) {
                    extract_from_param(param, result);
                }
                break;
            }
            case expressions::expression_group::sort: {
                auto* srt = static_cast<expressions::sort_expression_t*>(expr.get());
                result.insert(srt->key().as_string());
                break;
            }
            case expressions::expression_group::function: {
                auto* fn = static_cast<expressions::function_expression_t*>(expr.get());
                for (const auto& arg : fn->args()) {
                    extract_from_param(arg, result);
                }
                break;
            }
            default:
                break;
        }
        return result;
    }

    node_ptr plan_optimizer_t::optimize(node_ptr plan) {
        return pushdown_filter(plan);
    }

    node_ptr plan_optimizer_t::pushdown_filter(node_ptr node) {
        // Обход снизу вверх. Интерес представляют узлы node_aggregate_t с дочерним match и без group и sort то есть внешний чистый фильтр над одним источником
        if (!node) return node;

        // Recurse into children first (bottom-up)
        for (size_t i = 0; i < node->children().size(); ++i) {
            auto& child = node->children()[i];
            auto optimized = pushdown_filter(child);
            if (optimized != child) {
                node->children()[i] = optimized;
            }
        }

        // Only process node_aggregate_t
        if (node->type() != node_type::aggregate_t) return node;

        auto* agg = static_cast<node_aggregate_t*>(node.get());
        if (agg->children().size() < 2) return node;

        // Classify children: find match, group, sort
        node_ptr match_child = nullptr;
        node_ptr group_child = nullptr;
        node_ptr sort_child = nullptr;

        for (size_t i = 1; i < agg->children().size(); ++i) {
            auto& c = agg->children()[i];
            if (c->type() == node_type::match_t && !match_child) {
                match_child = c;
            }
            if (c->type() == node_type::group_t) group_child = c;
            if (c->type() == node_type::sort_t) sort_child = c;
        }

        // Only handle "pure filter" aggregates (match only, no group/sort)
        if (!match_child || group_child || sort_child) return node;

        auto source = agg->children()[0];

        // --- Rule: filter over sort ---
        if (source->type() == node_type::aggregate_t) {
            auto* source_agg = static_cast<node_aggregate_t*>(source.get());
            bool source_has_sort = false;
            bool source_has_group = false;
            bool source_has_match = false;

            bool source_has_select = false;
            for (size_t i = 1; i < source_agg->children().size(); ++i) {
                if (source_agg->children()[i]->type() == node_type::sort_t) source_has_sort = true;
                if (source_agg->children()[i]->type() == node_type::group_t) source_has_group = true;
                if (source_agg->children()[i]->type() == node_type::match_t) source_has_match = true;
                if (source_agg->children()[i]->type() == node_type::select_t) source_has_select = true;
            }

            // Filter over pure sort: always safe to push down
            if (source_has_sort && !source_has_group && !source_has_match) {
                // Move match_child into source_agg, remove from current agg
                source_agg->append_child(match_child);
                // Current agg becomes passthrough: replace with source
                return pushdown_filter(source);
            }

            // Filter over pure select: только «тождественные» столбцы, без sort/match/group рядом с select
            if (source_has_select && !source_has_sort && !source_has_group && !source_has_match) {
                node_ptr src_select_wrapped = nullptr;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::select_t) {
                        src_select_wrapped = source_agg->children()[i];
                        break;
                    }
                }
                if (src_select_wrapped && !match_child->expressions().empty()) {
                    auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                    std::set<std::string> input_cols;
                    collect_subtree_columns(source_agg->children()[0], input_cols);
                    auto* src_select = static_cast<node_select_t*>(src_select_wrapped.get());
                    if (filter_supported_through_identity_select(*src_select, filter_cols, input_cols)) {
                        source_agg->append_child(match_child);
                        return pushdown_filter(source);
                    }
                }
            }

            // Filter over projection (group with only scalar expressions, no aggregates)
            if (source_has_group && !source_has_sort && !source_has_match) {
                // Find the group node
                node_ptr src_group = nullptr;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::group_t) {
                        src_group = source_agg->children()[i];
                        break;
                    }
                }
                // Check if it's a projection (all scalar, no aggregate expressions)
                bool is_projection = true;
                std::set<std::string> output_cols;
                for (const auto& expr : src_group->expressions()) {
                    if (expr->group() == expressions::expression_group::aggregate) {
                        is_projection = false;
                        break;
                    }
                    if (expr->group() == expressions::expression_group::scalar) {
                        auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                        output_cols.insert(sc->key().as_string());
                    }
                }

                if (is_projection && !match_child->expressions().empty()) {
                    auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                    bool subset = std::includes(output_cols.begin(), output_cols.end(),
                                                filter_cols.begin(), filter_cols.end());
                    if (subset) {
                        source_agg->append_child(match_child);
                        return pushdown_filter(source);
                    }
                }
            }

            // Filter over groupBy (group with aggregate expressions)
            if (source_has_group && !source_has_sort && !source_has_match) {
                node_ptr src_group = nullptr;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::group_t) {
                        src_group = source_agg->children()[i];
                        break;
                    }
                }
                // Extract group keys (scalar expressions with group_field type)
                std::set<std::string> group_keys;
                for (const auto& expr : src_group->expressions()) {
                    if (expr->group() == expressions::expression_group::scalar) {
                        auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                        if (sc->type() == expressions::scalar_type::group_field) {
                            group_keys.insert(sc->key().as_string());
                        }
                    }
                }
                if (!group_keys.empty() && !match_child->expressions().empty()) {
                    auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                    bool subset = std::includes(group_keys.begin(), group_keys.end(),
                                                filter_cols.begin(), filter_cols.end());
                    if (subset) {
                        source_agg->append_child(match_child);
                        return pushdown_filter(source);
                    }
                }
            }
        }

        // --- Rule: filter over join ---
        if (source->type() == node_type::join_t) {
            auto* join = static_cast<node_join_t*>(source.get());
            if (join->children().size() >= 2) {
                std::set<std::string> left_cols, right_cols;
                collect_subtree_columns(join->children()[0], left_cols);
                collect_subtree_columns(join->children()[1], right_cols);

                if (!match_child->expressions().empty()) {
                    auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                    bool all_left = true, all_right = true;
                    for (const auto& c : filter_cols) {
                        if (left_cols.find(c) == left_cols.end()) all_left = false;
                        if (right_cols.find(c) == right_cols.end()) all_right = false;
                    }

                    if (all_left && !all_right) {
                        auto new_agg = make_node_aggregate(
                            node->resource(), join->children()[0]->collection_full_name());
                        new_agg->append_child(join->children()[0]);
                        new_agg->append_child(match_child);
                        join->children()[0] = boost::static_pointer_cast<node_t>(new_agg);
                        return pushdown_filter(source);
                    }
                    if (all_right && !all_left) {
                        auto new_agg = make_node_aggregate(
                            node->resource(), join->children()[1]->collection_full_name());
                        new_agg->append_child(join->children()[1]);
                        new_agg->append_child(match_child);
                        join->children()[1] = boost::static_pointer_cast<node_t>(new_agg);
                        return pushdown_filter(source);
                    }
                }
            }
        }

        return node;
    }

} // namespace components::logical_plan
