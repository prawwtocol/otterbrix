#include "pyrelation.hpp"

#include "pytype.hpp"

#include "pyexpression.hpp"

#include <connection_environment/connection_environment.hpp>

#include <core/string_util/string_util.hpp>
#include <components/logical_plan/node_join.hpp>

#include <optional>

using namespace components;

namespace otterbrix {

    namespace {
        std::optional<logical_plan::join_type> parse_join_type(const std::string& name) {
            if (name == "inner") {
                return logical_plan::join_type::inner;
            }
            if (name == "full") {
                return logical_plan::join_type::full;
            }
            if (name == "left") {
                return logical_plan::join_type::left;
            }
            if (name == "right") {
                return logical_plan::join_type::right;
            }
            if (name == "cross") {
                return logical_plan::join_type::cross;
            }
            if (name == "invalid") {
                return logical_plan::join_type::invalid;
            }
            return std::nullopt;
        }
    } // namespace

    PyRelation::PyRelation(ConnectionEnvironment* env, PlanFragment frag,
                           std::vector<std::shared_ptr<ExternalDependency>> deps)
        : executed(false), env(env),
          frag_(std::move(frag)),
          deps_(std::move(deps)),
          result(nullptr),
          optimize_(false) {
        if (!frag_.node) {
            throw std::runtime_error("PyRelation: null plan node");
        }
    }

    PyRelation::~PyRelation() {
        assert(py::gil_check());
        py::gil_scoped_release gil;
    }

    unique_ptr<PyRelation> PyRelation::Project(const py::args& args) {
        if (args.size() == 0) {
            return nullptr;
        }
        vector<Expression> fields;
        fields.reserve(args.size());
        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            }
            fields.push_back(py_expr->GetExpression());
        }
        auto next = env->BuildSelect(frag_, std::move(fields));
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }

    unique_ptr<PyRelation> PyRelation::Filter(const py::object &condition) {
        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("Implementation Error. Couldn\'t execute string expression");
        }
        pyexpr_ptr py_expr;
        if (!py::try_cast(condition, py_expr)) {
            throw std::runtime_error("Invalid Input Exception. Please provide either a string or a PyExpression object to \'filter\'");
        }

        const auto& expr = py_expr->GetExpression();
        auto next = env->BuildFilter(frag_, expr);
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }

    unique_ptr<PyRelation> PyRelation::Order(const string& arg) {
        auto* factory = GetExpressionFactory();
        auto expr = factory->SortExpression(arg);
        auto next = env->BuildSort(frag_, {expr});
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }

    unique_ptr<PyRelation> PyRelation::Sort(const py::args& args) {
        vector<Expression> order_nodes;
        order_nodes.reserve(args.size());

        auto* factory = GetExpressionFactory();
        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression();
                order_nodes.push_back(factory->SortExpression(expr));
            }
        }
        auto next = env->BuildSort(frag_, std::move(order_nodes));
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }


    unique_ptr<PyRelation> PyRelation::Group(const py::args& args) {
        vector<Expression> fields;
        fields.reserve(args.size());


        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression();
                fields.push_back(expr);
            }

        }
        auto next = env->BuildGroup(frag_, std::move(fields));
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }
    unique_ptr<PyRelation> PyRelation::Join(const PyRelation& other, const py::object& condition, const string& type) {
        auto type_string = string_utils::Lower(type);

        auto parse_result = parse_join_type(type_string);
        if (!parse_result.has_value()) {
            throw std::runtime_error("Couldn\'t parse the join type");
        }
        auto dtype = parse_result.value();

        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("OtterBrix couldn\'t parse condition. Please call join with an expression parameter");
        }

        vector<Expression> exprs;
        shared_ptr<PyExpression> py_expr;
        if (!condition.is_none()) {

            if (!py::try_cast<shared_ptr<PyExpression>>(condition, py_expr)) {
                throw std::runtime_error("Please provide condition as an expression either in string form or as an Expression object");
            }
            const auto& expr = py_expr->GetExpression();
            exprs.push_back(expr);
        } else {
            exprs.push_back(env->TrueExpression());
        }
        auto next = env->BuildJoin(frag_, other.frag_, std::move(exprs), dtype);
        std::vector<std::shared_ptr<ExternalDependency>> merged_deps = deps_;
        merged_deps.insert(merged_deps.end(),
                           other.deps_.begin(), other.deps_.end());
        return make_unique<PyRelation>(env, std::move(next), std::move(merged_deps));
    }


    unique_ptr<PyRelation> PyRelation::Cross(const PyRelation& other) {
        return Join(other, py::none(), "cross");
    }

    unique_ptr<PyRelation> PyRelation::Limit(int64_t count) {
        auto next = env->BuildLimit(frag_, count);
        return make_unique<PyRelation>(env, std::move(next), deps_);
    }

    cursor::cursor_t_ptr PyRelation::ExecuteInternal() {
        executed = true;
        if (!frag_.node) {
            return nullptr;
        }
        assert(py::gil_check());
        py::gil_scoped_release release;
        return env->Execute(frag_.node, optimize_);
    }


    void PyRelation::ExecuteOrThrow() {
        py::gil_scoped_acquire gil;
        result.reset();
        auto query_result = ExecuteInternal();
        if (!query_result) {
            throw std::runtime_error("ExecuteOrThrow - no query available to execute");
        }
        if (query_result->is_error()) {
            throw std::runtime_error(query_result->get_error().what.c_str());
        }
        result = make_unique<PyResult>(env, std::move(query_result), frag_.columns);
    }

    // Fetch

    Optional<py::tuple> PyRelation::FetchOne() {
        if (!result) {
            if (!frag_.node) {
                return py::none();
            }
            ExecuteOrThrow();
        }
        if (result->IsClosed()) {
            return py::none();
        }
        return result->Fetchone();
    }

    py::list PyRelation::FetchMany(idx_t size) {
        if (!result) {
            if (!frag_.node) {
                return py::list();
            }
            ExecuteOrThrow();
            assert(result);
        }
        if (result->IsClosed()) {
            return py::list();
        }
        return result->Fetchmany(size);
    }

    py::list PyRelation::FetchAll() {
        if (!result) {
            if (!frag_.node) {
                return py::list();
            }
            ExecuteOrThrow();
        }
        if (result->IsClosed()) {
            return py::list();
        }
        auto res = result->Fetchall();
        result = nullptr;
        return res;
    }

    PandasDataFrame PyRelation::FetchDF() {
        if (!result) {
            if (!frag_.node) {
                return py::list();
            }
            ExecuteOrThrow();
        }
        if (result->IsClosed()) {
            return py::list();
        }
        auto res = result->FetchDF();
        result = nullptr;
        return res;
    }

    pyexpr_ptr PyRelation::ColumnExpression(const string& name) {

        return make_shared<PyExpression>(expressions::key_t(std::pmr::get_default_resource(), name), GetExpressionFactory());
    }

    py::list PyRelation::Columns() {
        py::list res;
        for (auto& c : frag_.columns) {
            res.append(c.name());
        }
        return res;
    }

    py::list PyRelation::ColumnTypes() {
        py::list res;
        for (auto& c : frag_.columns) {
            res.append(OtterBrixPyType(c.type()));
        }
        return res;
    }

    // Internal functions (not exposed to Python)
    ExpressionFactory* PyRelation::GetExpressionFactory() {
        return static_cast<ExpressionFactory*>(env);
    }
} // namespace otterbrix
