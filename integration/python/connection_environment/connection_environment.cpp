#include "connection_environment.hpp"
#include <iostream>
#include <functional>
#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/planner/optimizer.hpp>
#include <scan/python_replacement_scan.hpp>
using namespace components;

namespace otterbrix {

    shared_ptr<PythonImportCache> ConnectionEnvironment::import_cache = nullptr;
    shared_ptr<case_insensitive_set_t> ConnectionEnvironment::collections = nullptr;

    namespace {
        static inline const std::string error_str = "#";

        static components::types::complex_logical_type find_type(
                std::string name,
                const std::vector<components::table::column_definition_t>& initial) {
            for (const auto& col : initial) {
                if (col.name() == name) return col.type();
            }
            return components::types::logical_type::UNKNOWN;
        }

        static std::pair<std::string, bool> find_param_name(
                const std::variant<core::parameter_id_t,
                                   components::expressions::key_t,
                                   components::expressions::expression_ptr>& param) {
            return std::visit([](const auto& expr) {
                using type = std::decay_t<decltype(expr)>;
                if constexpr (std::is_same_v<type, components::expressions::key_t>) {
                    return std::make_pair(expr.as_string(), true);
                } else if constexpr (std::is_same_v<type, core::parameter_id_t> ||
                                     std::is_same_v<type, components::expressions::expression_ptr>) {
                    return std::make_pair(error_str, false);
                }
                throw std::runtime_error("Unknown parameter type for nodes");
            }, param);
        }

        static components::table::column_definition_t process_aggregate(
                components::expressions::aggregate_expression_ptr aggregate_expr,
                const std::vector<components::table::column_definition_t>& initial) {
            std::string name = error_str;
            components::types::complex_logical_type type =
                components::types::logical_type::UNKNOWN;
            if (aggregate_expr->params().size() > 1) {
                return components::table::column_definition_t(name, type);
            }
            bool is_count = aggregate_expr->function_name() == "count";
            if (is_count) {
                name = "count";
                type = components::types::logical_type::UBIGINT;
            } else {
                const auto& param = aggregate_expr->params().front();
                auto founded_name = find_param_name(param);
                if (aggregate_expr->key().is_null()) {
                    std::string agg_str = aggregate_expr->function_name();
                    name = agg_str + "(" + founded_name.first + ")";
                } else {
                    name = aggregate_expr->key().as_string();
                }
                auto base_type = find_type(founded_name.first, initial);
                if (aggregate_expr->function_name() == "avg") {
                    type = components::types::logical_type::DOUBLE;
                } else {
                    type = base_type;
                }
            }
            return components::table::column_definition_t(name, type);
        }

        static components::table::column_definition_t process_scalar(
                components::expressions::scalar_expression_ptr scalar_expr,
                const std::vector<components::table::column_definition_t>& initial) {
            std::string name = error_str;
            components::types::complex_logical_type type =
                components::types::logical_type::UNKNOWN;
            if (scalar_expr->type() != components::expressions::scalar_type::get_field) {
                return components::table::column_definition_t(name, type);
            }
            if (scalar_expr->params().size() > 1) {
                return components::table::column_definition_t(name, type);
            }
            if (scalar_expr->params().size() == 1) {
                auto param_name = find_param_name(scalar_expr->params().front());
                name = scalar_expr->key().is_null() ? param_name.first
                                                    : scalar_expr->key().as_string();
                type = find_type(param_name.first, initial);
            } else {
                if (!scalar_expr->key().is_null()) {
                    name = scalar_expr->key().as_string();
                }
                type = find_type(name, initial);
            }
            return components::table::column_definition_t(name, type);
        }
    }

    boost::intrusive_ptr<otterbrix_t> ConnectionEnvironment::MakeSpace(
        const std::filesystem::path& path) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directory(path);
        return make_otterbrix(
            configuration::config::create_config(path)
            );
    }

    ConnectionEnvironment::ConnectionEnvironment() 
        : ConnectionEnvironment(MakeSpace()) {    
    }

    ConnectionEnvironment::ConnectionEnvironment(const boost::intrusive_ptr<otterbrix_t>& space)
        : ExpressionFactory(space), space(space) {
            auto session = otterbrix::session_id_t();
            space->dispatcher()->execute_sql(session, "CREATE DATABASE tmp;");
        }

    ConnectionEnvironment::~ConnectionEnvironment() = default;

    void ConnectionEnvironment::Cleanup() {
        import_cache.reset();
    }

    void ConnectionEnvironment::ThrowConnectionException() {
            throw std::runtime_error("Connection already closed!");
    }

    void ConnectionEnvironment::SetNullConnection() {
        space = nullptr;
        ExpressionFactory::SetNullSpace();
    }

    void ConnectionEnvironment::CreateDatabase(const std::string& name) {
        auto session = session_id_t();
        space->dispatcher()->execute_sql(session, "CREATE DATABASE " + name + ";");
    }

    Result ConnectionEnvironment::ExecuteInternal(const string& query) {
        using namespace components::sql::transform;

        auto session = session_id_t();
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));

        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (result.has_error()) {
            return components::cursor::make_cursor(space->dispatcher()->resource(), result.error());
        }
        auto plan = std::move(result).value();
        // Keep the main-query root for the post-execute catalog scan below;
        // the plan itself is consumed by execute_plan.
        auto root = plan.sub_queries.back();
        auto cursor = space->dispatcher()->execute_plan(session, std::move(plan));

        if (cursor->is_success()) {
            // CREATE TABLE plan is no longer a single create_collection node. It is wrapped in a sequence with a catalog_resolve_namespace node that holds the database name. Walk the plan to find the created collection and database. For queries without an explicit database, use the PostgreSQL default namespace "public" (same default as the engine search path).
            const logical_plan::node_create_collection_t* created = nullptr;
            std::string dbname;
            std::function<void(const logical_plan::node_ptr&)> scan =
                [&](const logical_plan::node_ptr& n) {
                    if (!n) {
                        return;
                    }
                    if (n->type() == logical_plan::node_type::create_collection_t) {
                        created = static_cast<const logical_plan::node_create_collection_t*>(n.get());
                    } else if (n->type() == logical_plan::node_type::catalog_resolve_namespace_t) {
                        dbname = static_cast<const logical_plan::node_catalog_resolve_namespace_t*>(n.get())->dbname();
                    }
                    for (const auto& child : n->children()) {
                        scan(child);
                    }
                };
            scan(root);
            if (created) {
                if (dbname.empty()) {
                    dbname = "public";
                }
                auto& collections = GetCollections();
                collections.insert(dbname + "." + created->relname());
            }
        }

        return cursor;
    }

    cursor::cursor_t_ptr ConnectionEnvironment::QueryRelation(const components::logical_plan::node_ptr &rel) {
        auto session = otterbrix::session_id_t();
        return space->dispatcher()->execute_plan(
            session,
            components::logical_plan::execution_plan_t{
                space->dispatcher()->resource(), rel, ExpressionFactory::GetParams()});
    }

    bool ConnectionEnvironment::IsJupyter() {
        return false;
    }


    PythonImportCache& ConnectionEnvironment::ImportCache() {
        if (!import_cache) {
            import_cache = make_shared<PythonImportCache>();
        }
        return *(import_cache.get());
    }

    case_insensitive_set_t& ConnectionEnvironment::GetCollections() {
        if (!collections) {
            collections = make_shared<case_insensitive_set_t>();
        }
        return *(collections.get());
    }

    components::cursor::cursor_t_ptr ConnectionEnvironment::Execute(
        components::logical_plan::node_ptr root, bool optimize) {
        auto session = session_id_t();
        if (optimize) {
            root = components::planner::optimize(root->resource(), root, nullptr);
        }
        return space->dispatcher()->execute_plan(
            session,
            components::logical_plan::execution_plan_t{
                root->resource(), root, ExpressionFactory::GetParams()});
    }

    std::pair<PlanFragment, std::shared_ptr<ExternalDependency>>
        ConnectionEnvironment::FromDataFrame(std::unique_ptr<components::tableref::TableRef> ref) {
        auto external_dependency = ref->external_dependency;
        auto tableData = Scan::FetchObjectData(
            space->dispatcher()->resource(), std::move(ref));
        PlanFragment frag;
        frag.node = boost::static_pointer_cast<components::logical_plan::node_t>(
            tableData.first);
        if (tableData.second) {
            frag.columns = std::move(*tableData.second);
        }
        return {std::move(frag), std::move(external_dependency)};
    }

    PlanFragment ConnectionEnvironment::FromSqlQuery(const string& query) {
        using namespace components::sql::transform;
        std::pmr::monotonic_buffer_resource parser_arena(
            space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(
            pg_cell_to_node_cast(parse_result)).finalize();
        if (result.has_error()) {
            throw std::runtime_error(result.error().what.c_str());
        }
        auto plan = std::move(result).value();
        PlanFragment frag;
        frag.node = plan.sub_queries.back();
        return frag;
    }

    PlanFragment ConnectionEnvironment::BuildFilter(PlanFragment child,
                                                    const Expression& condition) {
        auto resource = space->dispatcher()->resource();
        auto match_node = std::visit(
            [resource](const auto& expr) -> components::logical_plan::node_match_ptr {
                using T = std::decay_t<decltype(expr)>;
                if constexpr (std::is_same_v<T, components::expressions::expression_ptr>) {
                    if (expr->group() == components::expressions::expression_group::compare) {
                        return components::logical_plan::make_node_match(
                            resource, core::dbname_t{}, core::relname_t{}, expr);
                    } else if constexpr (std::is_same_v<T, components::types::logical_value_t> ||
                                         std::is_same_v<T, components::expressions::key_t>) {
                        throw std::runtime_error("The method supports only condition expressions");
                    }
                    throw std::runtime_error("Implementation Error. Undefined expression for filter");
                }
                throw std::runtime_error("The method supports only condition expression");
            }, condition);

        auto agg = components::logical_plan::make_node_aggregate(
            resource, core::dbname_t{"tmp"}, core::relname_t{});
        agg->append_child(child.node);
        agg->append_child(match_node);

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(agg),
            std::move(child.columns)};
    }

    PlanFragment ConnectionEnvironment::BuildSort(PlanFragment child,
                                                  std::vector<Expression> sort_exprs) {
        if (sort_exprs.empty()) {
            throw std::runtime_error("Please provide at least one expression to sort on");
        }
        auto resource = space->dispatcher()->resource();
        std::vector<components::expressions::expression_ptr> sort_expressions;
        sort_expressions.reserve(sort_exprs.size());
        for (auto& sort_expr_variant : sort_exprs) {
            std::visit([&](const auto& sort_expr) {
                using T = std::decay_t<decltype(sort_expr)>;
                if constexpr (std::is_same_v<T, components::expressions::expression_ptr>) {
                    if (sort_expr->group() == components::expressions::expression_group::sort) {
                        sort_expressions.push_back(sort_expr);
                    } else {
                        throw std::runtime_error("Undefined expression type for sort relation");
                    }
                } else if constexpr (std::is_same_v<T, components::types::logical_value_t> ||
                                     std::is_same_v<T, components::expressions::key_t>) {
                    throw std::runtime_error("The method supports only sort expressions");
                } else {
                    throw std::runtime_error("Implementation Error. Undefined expression type for sort relation");
                }
            }, sort_expr_variant);
        }
        auto sort_node = components::logical_plan::make_node_sort(
            resource, core::dbname_t{}, core::relname_t{}, std::move(sort_expressions));

        auto agg = components::logical_plan::make_node_aggregate(
            resource, core::dbname_t{"tmp"}, core::relname_t{});
        agg->append_child(child.node);
        agg->append_child(sort_node);

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(agg),
            std::move(child.columns)};
    }

    PlanFragment ConnectionEnvironment::BuildLimit(PlanFragment child, int64_t count) {
        auto resource = space->dispatcher()->resource();
        auto limit_node = components::logical_plan::make_node_limit(
            resource, core::dbname_t{}, core::relname_t{},
            components::logical_plan::limit_t(count));

        auto agg = components::logical_plan::make_node_aggregate(
            resource, core::dbname_t{"tmp"}, core::relname_t{});
        agg->append_child(child.node);
        agg->append_child(limit_node);

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(agg),
            std::move(child.columns)};
    }

    PlanFragment ConnectionEnvironment::BuildGroup(PlanFragment child,
                                                   std::vector<Expression> fields) {
        auto resource = space->dispatcher()->resource();
        std::vector<components::expressions::expression_ptr> exprs;
        exprs.reserve(fields.size());
        for (auto& field_variant : fields) {
            exprs.push_back(
                std::visit([resource](const auto& field)
                    -> components::expressions::expression_ptr {
                    using T = std::decay_t<decltype(field)>;
                    if constexpr (std::is_same_v<T, components::expressions::expression_ptr>) {
                        if (field->group() == components::expressions::expression_group::aggregate) {
                            return field;
                        } else if (field->group() == components::expressions::expression_group::scalar) {
                            auto scalar = boost::static_pointer_cast<
                                components::expressions::scalar_expression_t>(field);
                            if (scalar->type() == components::expressions::scalar_type::get_field) {
                                return scalar;
                            } else {
                                throw std::runtime_error("Could\'t use scalar expression in a group node");
                            }
                        } else {
                            throw std::runtime_error("Undefined expression type for group relation");
                        }
                    } else if constexpr (std::is_same_v<T, components::expressions::key_t>) {
                        return components::expressions::make_scalar_expression(
                            resource, components::expressions::scalar_type::get_field, field);
                    } else if constexpr (std::is_same_v<T, components::types::logical_value_t>) {
                        throw std::runtime_error("The method supports only aggregation expressions and fields");
                    } else {
                        throw std::runtime_error("Implementation Error. Undefined expression type for group relation");
                    }
                }, field_variant));
        }
        auto group_node = components::logical_plan::make_node_group(
            resource, core::dbname_t{}, core::relname_t{}, std::move(exprs));

        std::vector<components::table::column_definition_t> out_cols;
        const auto& group_exprs = group_node->expressions();
        out_cols.reserve(group_exprs.size());
        for (const auto& expr : group_exprs) {
            using namespace components::expressions;
            switch (expr->group()) {
                case expression_group::aggregate:
                    out_cols.push_back(process_aggregate(
                        boost::static_pointer_cast<aggregate_expression_t>(expr),
                        child.columns));
                    break;
                case expression_group::scalar:
                    out_cols.push_back(process_scalar(
                        boost::static_pointer_cast<scalar_expression_t>(expr),
                        child.columns));
                    break;
                default:
                    out_cols.emplace_back(error_str, components::types::logical_type::UNKNOWN);
            }
        }

        auto agg = components::logical_plan::make_node_aggregate(
            resource, core::dbname_t{"tmp"}, core::relname_t{});
        agg->append_child(child.node);
        agg->append_child(group_node);

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(agg),
            std::move(out_cols)};
    }

    PlanFragment ConnectionEnvironment::BuildSelect(PlanFragment child,
                                                    std::vector<Expression> fields) {
        auto resource = space->dispatcher()->resource();
        auto select_node = components::logical_plan::make_node_select(
            resource, core::dbname_t{}, core::relname_t{});
        std::vector<components::table::column_definition_t> out_cols;
        out_cols.reserve(fields.size());

        for (auto& field_variant : fields) {
            auto scalar = std::visit([resource](const auto& field)
                -> components::expressions::expression_ptr {
                using T = std::decay_t<decltype(field)>;
                if constexpr (std::is_same_v<T, components::expressions::expression_ptr>) {
                    if (field->group() == components::expressions::expression_group::scalar) {
                        return field;
                    }
                    if (field->group() == components::expressions::expression_group::aggregate) {
                        throw std::runtime_error(
                            "Aggregate expressions are not allowed in select(); use groupBy().agg() instead");
                    }
                    throw std::runtime_error("Undefined expression type for select relation");
                } else if constexpr (std::is_same_v<T, components::expressions::key_t>) {
                    return components::expressions::make_scalar_expression(
                        resource, components::expressions::scalar_type::get_field, field);
                } else if constexpr (std::is_same_v<T, components::types::logical_value_t>) {
                    throw std::runtime_error("The method supports only column expressions and fields");
                } else {
                    throw std::runtime_error("Implementation Error. Undefined expression type for select relation");
                }
            }, field_variant);

            select_node->append_expression(scalar);
            out_cols.push_back(process_scalar(
                boost::static_pointer_cast<components::expressions::scalar_expression_t>(scalar),
                child.columns));
        }

        auto agg = components::logical_plan::make_node_aggregate(
            resource, core::dbname_t{"tmp"}, core::relname_t{});
        agg->append_child(child.node);
        agg->append_child(select_node);

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(agg),
            std::move(out_cols)};
    }

    PlanFragment ConnectionEnvironment::BuildJoin(PlanFragment left, PlanFragment right,
                                                  std::vector<Expression> conditions,
                                                  components::logical_plan::join_type type) {
        std::vector<components::expressions::expression_ptr> cond_exprs;
        cond_exprs.reserve(conditions.size());
        for (auto& cond_variant : conditions) {
            cond_exprs.push_back(
                std::visit([](const auto& cond_expr)
                    -> components::expressions::expression_ptr {
                    using T = std::decay_t<decltype(cond_expr)>;
                    if constexpr (std::is_same_v<T, components::expressions::expression_ptr>) {
                        if (cond_expr->group() == components::expressions::expression_group::compare) {
                            return cond_expr;
                        } else {
                            throw std::runtime_error("Undefined expression type for sort relation");
                        }
                    } else if constexpr (std::is_same_v<T, components::types::logical_value_t> ||
                                         std::is_same_v<T, components::expressions::key_t>) {
                        throw std::runtime_error("The method supports only conditions");
                    } else {
                        throw std::runtime_error("Implementation Error. Undefined expression type for condition");
                    }
                }, cond_variant));
        }

        auto resource = space->dispatcher()->resource();
        auto join_node = components::logical_plan::make_node_join(
            resource, core::dbname_t{}, core::relname_t{}, type);
        join_node->append_child(left.node);
        join_node->append_child(right.node);
        for (auto& expr : cond_exprs) {
            join_node->append_expression(expr);
        }

        std::vector<components::table::column_definition_t> out_cols;
        out_cols.reserve(left.columns.size() + right.columns.size());
        for (auto& c : left.columns)  out_cols.emplace_back(c.name(), c.type());
        for (auto& c : right.columns) out_cols.emplace_back(c.name(), c.type());

        return PlanFragment{
            boost::static_pointer_cast<components::logical_plan::node_t>(join_node),
            std::move(out_cols)};
    }


} // namespace otterbrix
