#include "convert_value.hpp"

#include <native/python_objects.hpp>
#include <core/typedefs.hpp>
#include <core/types/string.hpp>

#include <stdexcept>

using components::types::physical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::types::complex_logical_type;

namespace otterbrix {

    namespace util {

        py::object LogicalValueToPython(const logical_value_t& value,
                const complex_logical_type& type) {
            if (value.is_null()) {
                return py::none();
            }
            return PythonObject::FromValue(value, type);
        }

        py::dict CursorRowToPythonDict(components::cursor::cursor_t_ptr& cursor,
                uint64_t row_idx,
                const vector<components::table::column_definition_t>& col_defs) {
            py::dict result;
            for (idx_t i = 0; i < col_defs.size(); i++) {
                auto val = cursor->value(i, row_idx);
                result[py::str(col_defs[i].name())] = LogicalValueToPython(val, col_defs[i].type());
            }
            return result;
        }

        py::dict DataChunkRowToPythonDict(const components::vector::data_chunk_t& chunk,
                uint64_t row_idx,
                const vector<components::table::column_definition_t>& col_defs) {
            py::dict result;
            for (idx_t i = 0; i < col_defs.size(); i++) {
                auto val = chunk.value(i, row_idx);
                result[py::str(col_defs[i].name())] = LogicalValueToPython(val, col_defs[i].type());
            }
            return result;
        }

    } // namespace util

} // namespace otterbrix
