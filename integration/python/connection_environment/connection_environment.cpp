#include "connection_environment.hpp"
#include <iostream>
#include <functional>
#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/logical_plan/optimizer.hpp>
using namespace components;

namespace otterbrix {

    shared_ptr<PythonImportCache> ConnectionEnvironment::import_cache = nullptr;
    shared_ptr<case_insensitive_set_t> ConnectionEnvironment::collections = nullptr; 

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
        : ExpressionFactory(space), RelationFactory(space), space(space) {
            auto session = otterbrix::session_id_t();
            space->dispatcher()->create_database(session, "tmp");
        }

    ConnectionEnvironment::~ConnectionEnvironment() = default;

    void ConnectionEnvironment::Cleanup() {
        import_cache.reset();
    }

    void ConnectionEnvironment::ThrowConnectionException() {
           // throw PyConnectionException("Connection already closed!");
            throw std::runtime_error("Connection already closed!");
    }

    void ConnectionEnvironment::SetNullConnection() {
        space = nullptr;
        ExpressionFactory::SetNullSpace();
    }

    void ConnectionEnvironment::CreateDatabase(const std::string& name) {
        auto session = session_id_t();
        space->dispatcher()->create_database(session, name);
    }

    shared_ptr<Relation> ConnectionEnvironment::RelationFromQuery(const string& query) {
        // parse sql, transform to AST
        using namespace components::sql::transform;

        // call parser with explicit memory arena, normalize with linitial to get the root ast element for transform
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        // parse sql result

        // convert ast to logical plan
        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (std::holds_alternative<bind_error>(result)) {
            throw std::runtime_error(std::get<bind_error>(std::move(result)).what());
        }
        auto view = std::get<result_view>(std::move(result));
        return RelationFactory::CreateFromSelect(std::move(view.node));
    }

    Result ConnectionEnvironment::ExecuteInternal(const string& query) {
        using namespace components::sql::transform;

        auto session = session_id_t();
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));

        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (std::holds_alternative<bind_error>(result)) {
            return components::cursor::make_cursor(space->dispatcher()->resource(),
                                                   components::cursor::error_code_t::sql_parse_error,
                                                   std::get<bind_error>(std::move(result)).what());
        }
        auto view = std::get<result_view>(std::move(result));
        auto plan = std::move(view.node);
        auto cursor = space->dispatcher()->execute_plan(session, plan, std::move(view.params));

        if (cursor->is_success() && plan->type() == logical_plan::node_type::create_collection_t) {
            auto full_name = plan->collection_full_name();
            auto& collections = GetCollections();
            collections.insert(full_name.to_string());
        }

        return cursor;
    }

    Result ConnectionEnvironment::Execute(const Relation& rel, bool optimize) {
        auto session = session_id_t();
        auto resource = space->dispatcher()->resource();

        // Recursively flatten the Relation tree bottom-up,
        // executing each step and materializing results into node_data_t
        std::function<logical_plan::node_data_ptr(const Relation&)> flatten;
        flatten = [&](const Relation& r) -> logical_plan::node_data_ptr {
            return std::visit([&](const auto& v) -> logical_plan::node_data_ptr {
                using T = std::decay_t<decltype(v)>;

                if constexpr (std::is_same_v<T, Relation::Data>) {
                    return v.data;  // base case
                }
                else if constexpr (std::is_same_v<T, Relation::Aggregate>) {
                    auto child_data = flatten(*v.resource);
                    auto agg = logical_plan::make_node_aggregate(resource, {"tmp", v.name});
                    agg->append_child(boost::static_pointer_cast<logical_plan::node_t>(child_data));
                    if (v.group) agg->append_child(v.group);
                    if (v.match) agg->append_child(v.match);
                    if (v.sort)  agg->append_child(v.sort);
                    logical_plan::node_ptr plan = boost::static_pointer_cast<logical_plan::node_t>(agg);
                    if (optimize) {
                        logical_plan::plan_optimizer_t opt;
                        plan = opt.optimize(plan);
                    }
                    auto cursor = space->dispatcher()->execute_plan(
                        session, plan, ExpressionFactory::GetParams());
                    if (!cursor || cursor->is_error())
                        throw std::runtime_error(cursor ? cursor->get_error().what : "Execution failed");
                    return logical_plan::make_node_raw_data(resource, std::move(cursor->chunk_data()));
                }
                else if constexpr (std::is_same_v<T, Relation::Join>) {
                    auto left = flatten(*v.left);
                    auto right = flatten(*v.right);
                    auto jn = logical_plan::make_node_join(resource, {}, v.join_type);
                    jn->append_child(boost::static_pointer_cast<logical_plan::node_t>(left));
                    jn->append_child(boost::static_pointer_cast<logical_plan::node_t>(right));
                    if (v.conditions)
                        for (const auto& e : *v.conditions) jn->append_expression(e);
                    auto cursor = space->dispatcher()->execute_plan(
                        session, boost::static_pointer_cast<logical_plan::node_t>(jn), ExpressionFactory::GetParams());
                    if (!cursor || cursor->is_error())
                        throw std::runtime_error(cursor ? cursor->get_error().what : "Execution failed");
                    return logical_plan::make_node_raw_data(resource, std::move(cursor->chunk_data()));
                }
                else if constexpr (std::is_same_v<T, Relation::Limit>) {
                    auto child = flatten(*v.resource);
                    auto ln = logical_plan::make_node_limit(resource, {}, limit_t(v.count));
                    ln->append_child(boost::static_pointer_cast<logical_plan::node_t>(child));
                    auto cursor = space->dispatcher()->execute_plan(
                        session, boost::static_pointer_cast<logical_plan::node_t>(ln), ExpressionFactory::GetParams());
                    if (!cursor || cursor->is_error())
                        throw std::runtime_error(cursor ? cursor->get_error().what : "Execution failed");
                    return logical_plan::make_node_raw_data(resource, std::move(cursor->chunk_data()));
                }
                throw std::runtime_error("Unknown relation type");
            }, r.relation);
        };

        // Flatten to data, then execute the final data node to get cursor
        auto data = flatten(rel);
        return space->dispatcher()->execute_plan(
            session, boost::static_pointer_cast<logical_plan::node_t>(data), ExpressionFactory::GetParams());
    }

    cursor::cursor_t_ptr ConnectionEnvironment::QueryRelation(const components::logical_plan::node_ptr &rel) {
        auto session = otterbrix::session_id_t();
        return space->dispatcher()->execute_plan(session, rel, ExpressionFactory::GetParams());
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

    
} // namespace otterbrix
