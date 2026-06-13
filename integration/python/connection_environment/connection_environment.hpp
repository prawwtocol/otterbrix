#pragma once

#include "expression/expression_factory.hpp"
#include "module_cheker.hpp"
#include "import_cache/python_import_cache.hpp"

#include <core/types/string.hpp>
#include <components/cursor/cursor.hpp>


#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/table/column_definition.hpp>
#include <components/tableref/tableref.hpp>
#include <core/external_dependencies.hpp>
#include <core/string_util/case_insensitive.hpp>

#include <integration/cpp/otterbrix.hpp>


#include <filesystem>
#include <string_view>
#include <utility>
#include <vector>

namespace otterbrix {
    using Result = components::cursor::cursor_t_ptr;

    struct PlanFragment {
        components::logical_plan::node_ptr node;
        std::vector<components::table::column_definition_t> columns;
    };

    class ConnectionEnvironment :
        public ExpressionFactory {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";
        static boost::intrusive_ptr<otterbrix_t> MakeSpace(const std::filesystem::path& 
            path = std::filesystem::current_path() / DEFAULT_FOLDER);
        // create default space
        ConnectionEnvironment();
        ConnectionEnvironment(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~ConnectionEnvironment();
    public:
        static void Cleanup();
        static void ThrowConnectionException();
        void SetNullConnection();

        void CreateDatabase(const string& name);
        Result ExecuteInternal(const string& query);

        PlanFragment BuildFilter(PlanFragment child, const Expression& condition);
        PlanFragment BuildGroup (PlanFragment child, std::vector<Expression> fields);
        PlanFragment BuildSort  (PlanFragment child, std::vector<Expression> sort_exprs);
        PlanFragment BuildSelect(PlanFragment child, std::vector<Expression> fields);
        PlanFragment BuildJoin  (PlanFragment left, PlanFragment right,
                                 std::vector<Expression> conditions,
                                 components::logical_plan::join_type type);
        PlanFragment BuildLimit (PlanFragment child, int64_t count);

        std::pair<PlanFragment, std::shared_ptr<ExternalDependency>>
            FromDataFrame(std::unique_ptr<components::tableref::TableRef> ref);

        PlanFragment FromSqlQuery(const string& query);

        components::cursor::cursor_t_ptr Execute(
            components::logical_plan::node_ptr root, bool optimize = false);

        components::cursor::cursor_t_ptr QueryRelation(const components::logical_plan::node_ptr &rel);
    public:
        static bool IsJupyter();

        static PythonImportCache& ImportCache();
        static case_insensitive_set_t& GetCollections();
        static case_insensitive_set_t& GetTables();
    
    private:
        boost::intrusive_ptr<otterbrix_t> space;   


    private:
        static shared_ptr<PythonImportCache> import_cache;
        static shared_ptr<case_insensitive_set_t> collections;

    };
} // namespace otterbrix
