#include "box_render.hpp"

#include <util/util.hpp>
#include <components/types/types.hpp>
#include <tabulate/table.hpp>

#include <stdexcept>


/**
Рендер таблицы (ASCII box) для курсора переведён с документа на курсор + logical_value_t.

Было: цикл до 100 итераций с cursor->next(), по каждой строке — объект value (document) и большой switch по logical_type: чтение через get_bool / get_int / … по json_pointer из имени столбца.

Стало:

цикл while (idx < 100 && cursor->has_next());
cursor->advance() вместо next();
для каждого столбца cursor->value(col) и util::LogicalValueToString(val) — единый вывод без перечисления типов вручную.
Подключён #include <util/util.hpp> для LogicalValueToString.

Зачем: тот же переход, что в pyresult.cpp — курсор отдаёт значения как logical_value, без document API.
*/
namespace otterbrix {

    using namespace tabulate;
    using components::types::logical_type;

    string Show(components::cursor::cursor_t_ptr cursor,
            const vector<components::table::column_definition_t>& col_defs) {
        Table box;
        Table::Row_t head;
        for (const auto& col_def : col_defs) {
            head.push_back(col_def.name());
        }
        box.add_row(head);
        box.column(0).format().font_align(FontAlign::center);

        int idx = 0;
        while (idx < 100 && cursor->has_next()) {
            cursor->advance();
            Table::Row_t row;

            for (idx_t col = 0; col < col_defs.size(); col++) {
                auto val = cursor->value(col);
                row.push_back(util::LogicalValueToString(val));
            }

            box.add_row(row);
            idx++;
        }
        return box.str();
    }
} // namespace otterbrix
