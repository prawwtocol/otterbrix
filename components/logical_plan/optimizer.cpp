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
            if (std::holds_alternative<expressions::key_t>(param)) {
                result.insert(std::get<expressions::key_t>(param).as_string());
            } else if (std::holds_alternative<expressions::expression_ptr>(param)) {
                auto cols = collect_referenced_columns(std::get<expressions::expression_ptr>(param));
                result.insert(cols.begin(), cols.end());
            }
        }

        void collect_subtree_columns(const node_ptr& node, std::set<std::string>& cols) {
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

        std::vector<expressions::expression_ptr>
        split_conjuncts(const expressions::expression_ptr& expr) {
            std::vector<expressions::expression_ptr> result;
            if (!expr) {
                return result;
            }
            if (expr->group() == expressions::expression_group::compare) {
                auto* cmp = static_cast<expressions::compare_expression_t*>(expr.get());
                if (cmp->type() == expressions::compare_type::union_and) {
                    for (const auto& child : cmp->children()) {
                        auto sub = split_conjuncts(child);
                        result.insert(result.end(), sub.begin(), sub.end());
                    }
                    return result;
                }
            }
            result.push_back(expr);
            return result;
        }

        expressions::expression_ptr
        rebuild_conjunction(std::pmr::memory_resource* resource,
                            const std::vector<expressions::expression_ptr>& conjuncts) {
            if (conjuncts.empty()) {
                return nullptr;
            }
            if (conjuncts.size() == 1) {
                return conjuncts.front();
            }
            auto conj = expressions::make_compare_union_expression(
                resource, expressions::compare_type::union_and);
            for (const auto& c : conjuncts) {
                conj->append_child(c);
            }
            return conj;
        }

        size_t type_width(const components::types::complex_logical_type& t) {
            return t.size();
        }

        const node_data_t* find_data_node(const node_ptr& node) {
            if (!node) {
                return nullptr;
            }
            if (node->type() == node_type::data_t) {
                return static_cast<const node_data_t*>(node.get());
            }
            for (const auto& child : node->children()) {
                if (auto* found = find_data_node(child)) {
                    return found;
                }
            }
            return nullptr;
        }

        size_t estimate_row_width(const node_ptr& node) {
            const node_data_t* data = find_data_node(node);
            if (!data) {
                return 0;
            }
            size_t width = 0;
            for (const auto& t : data->data_chunk().types()) {
                width += type_width(t);
            }
            return width;
        }

        size_t estimate_projection_width(const node_select_t& sel, const node_ptr& subtree) {
            const node_data_t* data = find_data_node(subtree);
            if (!data) {
                return 0;
            }
            const auto types = data->data_chunk().types();
            const auto& exprs = sel.expressions();
            const size_t hidden = sel.internal_aggregate_count;
            if (exprs.size() < hidden) {
                return 0;
            }
            const size_t visible = exprs.size() - hidden;
            size_t width = 0;
            for (size_t i = 0; i < visible; ++i) {
                const auto& expr = exprs[i];
                if (expr->group() != expressions::expression_group::scalar) {
                    return 0;
                }
                auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                const std::string out_name = sc->key().as_string();
                bool found = false;
                for (const auto& t : types) {
                    if (t.alias() == out_name) {
                        width += type_width(t);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return 0;
                }
            }
            return width;
        }

    } // anonymous namespace

    std::set<std::string> collect_referenced_columns(const expressions::expression_ptr& expr) {
        std::set<std::string> result;
        if (!expr) return result;

        switch (expr->group()) {
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
        if (!node) return node;

        for (size_t i = 0; i < node->children().size(); ++i) {
            auto& child = node->children()[i];
            auto optimized = pushdown_filter(child);
            if (optimized != child) {
                node->children()[i] = optimized;
            }
        }

        if (node->type() != node_type::aggregate_t) return node;

        auto* agg = static_cast<node_aggregate_t*>(node.get());
        if (agg->children().size() < 2) return node;

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

        if (!match_child || group_child || sort_child) return node;

        auto source = agg->children()[0];

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

            if (source_has_sort && !source_has_group && !source_has_match) {
                source_agg->append_child(match_child);
                return pushdown_filter(source);
            }

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
                        size_t width_full = estimate_row_width(source_agg->children()[0]);
                        size_t width_proj = estimate_projection_width(*src_select, source_agg->children()[0]);
                        bool cost_ok = width_full == 0 || width_proj == 0 || width_full <= width_proj;
                        if (cost_ok) {
                            source_agg->append_child(match_child);
                            return pushdown_filter(source);
                        }
                    }
                }
            }

            if (source_has_group && !source_has_sort && !source_has_match) {
                node_ptr src_group = nullptr;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::group_t) {
                        src_group = source_agg->children()[i];
                        break;
                    }
                }
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

            if (source_has_group && !source_has_sort && !source_has_match) {
                node_ptr src_group = nullptr;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::group_t) {
                        src_group = source_agg->children()[i];
                        break;
                    }
                }
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
                    auto conjuncts = split_conjuncts(match_child->expressions()[0]);
                    std::vector<expressions::expression_ptr> pushable, residual;
                    for (const auto& conj : conjuncts) {
                        auto cols = collect_referenced_columns(conj);
                        if (!cols.empty() &&
                            std::includes(group_keys.begin(), group_keys.end(),
                                          cols.begin(), cols.end())) {
                            pushable.push_back(conj);
                        } else {
                            residual.push_back(conj);
                        }
                    }
                    if (!pushable.empty()) {
                        source_agg->append_child(make_node_match(
                            node->resource(), match_child->collection_full_name(),
                            rebuild_conjunction(node->resource(), pushable)));
                        auto residual_expr = rebuild_conjunction(node->resource(), residual);
                        if (!residual_expr) {
                            return pushdown_filter(source);
                        }
                        match_child->expressions()[0] = residual_expr;
                        node->children()[0] = pushdown_filter(source);
                        return node;
                    }
                }
            }
        }

        if (source->type() == node_type::join_t) {
            auto* join = static_cast<node_join_t*>(source.get());
            if (join->children().size() >= 2 && !match_child->expressions().empty()) {
                std::set<std::string> left_cols, right_cols;
                collect_subtree_columns(join->children()[0], left_cols);
                collect_subtree_columns(join->children()[1], right_cols);

                auto conjuncts = split_conjuncts(match_child->expressions()[0]);
                std::vector<expressions::expression_ptr> left_bucket, right_bucket, residual;
                for (const auto& conj : conjuncts) {
                    auto cols = collect_referenced_columns(conj);
                    bool in_left = !cols.empty() &&
                        std::includes(left_cols.begin(), left_cols.end(),
                                      cols.begin(), cols.end());
                    bool in_right = !cols.empty() &&
                        std::includes(right_cols.begin(), right_cols.end(),
                                      cols.begin(), cols.end());
                    if (in_left && !in_right) {
                        left_bucket.push_back(conj);
                    } else if (in_right && !in_left) {
                        right_bucket.push_back(conj);
                    } else {
                        residual.push_back(conj);
                    }
                }

                if (!left_bucket.empty() || !right_bucket.empty()) {
                    auto coll = match_child->collection_full_name();
                    if (!left_bucket.empty()) {
                        auto new_agg = make_node_aggregate(
                            node->resource(), join->children()[0]->collection_full_name());
                        new_agg->append_child(join->children()[0]);
                        new_agg->append_child(make_node_match(
                            node->resource(), coll,
                            rebuild_conjunction(node->resource(), left_bucket)));
                        join->children()[0] = boost::static_pointer_cast<node_t>(new_agg);
                    }
                    if (!right_bucket.empty()) {
                        auto new_agg = make_node_aggregate(
                            node->resource(), join->children()[1]->collection_full_name());
                        new_agg->append_child(join->children()[1]);
                        new_agg->append_child(make_node_match(
                            node->resource(), coll,
                            rebuild_conjunction(node->resource(), right_bucket)));
                        join->children()[1] = boost::static_pointer_cast<node_t>(new_agg);
                    }
                    auto residual_expr = rebuild_conjunction(node->resource(), residual);
                    if (!residual_expr) {
                        return pushdown_filter(source);
                    }
                    match_child->expressions()[0] = residual_expr;
                    node->children()[0] = pushdown_filter(source);
                    return node;
                }
            }
        }

        return node;
    }

} // namespace components::logical_plan
