#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::operators {

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        size_t count = 0;
        std::cerr << "[match] on_execute_impl start, limit_check=" << limit_.check(0)
                  << " left_=" << (left_ ? 1 : 0) << std::endl;
        if (!limit_.check(static_cast<int>(count))) {
            std::cerr << "[match] limit=0, returning" << std::endl;
            return; //limit = 0
        }
        if (!left_) {
            std::cerr << "[match] no left_, returning" << std::endl;
            return;
        }
        if (left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            std::cerr << "[match] chunk.size=" << chunk.size() << " cols=" << chunk.column_count() << std::endl;
            auto types = chunk.types();
            output_ = operators::make_operator_data(left_->output()->resource(), types, chunk.size());
            auto& out_chunk = output_->data_chunk();
            std::cerr << "[match] creating predicate, has_expr=" << (expression_ ? 1 : 0)
                      << " has_func_reg=" << (pipeline_context->function_registry ? 1 : 0) << std::endl;
            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types,
                                                                        types,
                                                                        &pipeline_context->parameters)
                                         : predicates::create_all_true_predicate(left_->output()->resource());
            std::cerr << "[match] predicate created, filtering " << chunk.size() << " rows" << std::endl;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    for (size_t j = 0; j < chunk.column_count(); j++) {
                        out_chunk.set_value(j, count, chunk.data[j].value(i));
                    }
                    out_chunk.row_ids.data<int64_t>()[count] = chunk.row_ids.data<int64_t>()[i];
                    ++count;
                    if (!limit_.check(static_cast<int>(count))) {
                        out_chunk.set_cardinality(count);
                        return;
                    }
                }
            }
            out_chunk.set_cardinality(count);
        }
    }

} // namespace components::operators
