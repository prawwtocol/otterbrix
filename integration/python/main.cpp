
/**
 * ## Анализ двух файлов

### `main.cpp` — оригинальный Python-биндинг

Этот файл — **первоначальная** реализация Python-модуля `otterbrix` через pybind11. Он напрямую оборачивает внутренние C++ классы проекта:

- **`Client`** — обёртка над `wrapper_client`. Создаётся через синглтон `spaces::get_instance()`, может принимать строку конфигурации. Имеет метод `execute`.
- **`Connection`** — обёртка над `wrapper_connection`. Создаётся из `Client`. Предоставляет методы `execute`, `cursor`, `close`, `commit`, `rollback` — классический DB-API-подобный интерфейс.
- **`Cursor`** — обёртка над `wrapper_cursor` (хранится через `boost::intrusive_ptr`). Это наиболее развитый класс: поддерживает Python-протоколы (`__repr__`, `__iter__`, `__next__`, `__getitem__`, `__len__`), а также DB-API-методы (`fetchone`, `fetchmany`, `fetchall`, `description`, `rowcount`).
- Модульная функция **`connect(dsn)`** — удобная точка входа, создающая клиент и соединение за один вызов.
- Функция **`to_aggregate`** — утилита для тестирования SQL-конвертации.

Зависимости: использует заголовки из `sql/` — `wrapper_client.hpp`, `wrapper_cursor.hpp`, `wrapper_connection.hpp`, `convert.hpp`, `spaces.hpp`.

---

### `main2.cpp` — новая (переработанная) архитектура биндинга

Этот файл представляет собой **переработанный** подход. Уже в первой строке стоит комментарий `// todo is this file needed?`, что говорит о том, что это экспериментальная/промежуточная версия.

Ключевые отличия:

- Используется **другой набор абстракций**: `PyExpression`, `PyRelation`, `PyConnection`, `OtterBrixPyTyping`, `TypeCreation` — классы из каталога `otterbrix_wrapper/` и `pyconnection/`.
- Каждый компонент инициализируется через статический метод `::Initialize(m)`, что указывает на **модульную архитектуру** — каждый класс сам регистрирует свои биндинги.
- Функция **`connect`** принимает другие параметры: `database` (имя файла БД), `read_only` (флаг «только чтение»), `config` (словарь Python). Это более продвинутый API по сравнению с `main.cpp`, где `connect` принимает только DSN-строку.
- Есть **деструктор модуля** (`_clean_default_connection`) через `py::capsule` — корректная очистка ресурсов при выгрузке модуля.
- Тестовая функция `add` использует внутренний тип `complex_logical_type` — скорее всего, для проверки, что система типов корректно линкуется.
- Имя модуля задаётся через **макрос** `OTTERBRIX_PYTHON_LIB_NAME`, а не хардкодится.

---

### Сводка различий

| Аспект | `main.cpp` | `main2.cpp` |
|---|---|---|
| **Архитектура** | Монолитная — все биндинги в одном файле | Модульная — каждый класс сам регистрирует себя через `Initialize()` |
| **Классы** | `wrapper_client`, `wrapper_connection`, `wrapper_cursor` | `PyConnection`, `PyExpression`, `PyRelation`, `PyType`, `Typing` |
| **Управление памятью** | `boost::intrusive_ptr` для курсоров | Управление передано внутрь классов-обёрток |
| **`connect()`** | Принимает `dsn` (строку) | Принимает `database`, `read_only`, `config` |
| **Очистка ресурсов** | Отсутствует | Деструктор модуля через `py::capsule` |
| **Имя модуля** | Хардкод `otterbrix` | Макрос `OTTERBRIX_PYTHON_LIB_NAME` |
| **Зависимости** | Каталог `sql/` | Каталоги `otterbrix_wrapper/`, `pyconnection/` |

По сути, `main2.cpp` — это **следующее поколение** Python-интерфейса, с более чистой архитектурой, расширенными возможностями подключения и правильным управлением жизненным циклом. `main.cpp` — старая версия, привязанная к другому набору C++ обёрток.

*/

#include <pybind11/pybind_wrapper.hpp>

#include <otterbrix_wrapper/pyexpression.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/typing.hpp>
#include <otterbrix_wrapper/pytype.hpp>
#include <otterbrix_wrapper/type_creation.hpp>
#include <pyconnection/pyconnection.hpp>

#ifndef OTTERBRIX_PYTHON_LIB_NAME
#define OTTERBRIX_PYTHON_LIB_NAME otterbrix
#endif

using namespace otterbrix;

int add(int i, int j) {
    components::types::complex_logical_type lt(components::types::logical_type::INTEGER);
    return i + j + lt.size();
}


PYBIND11_MODULE(OTTERBRIX_PYTHON_LIB_NAME, m) {
    m.def("add", &add, "test"); 
    OtterBrixPyTyping::Initialize(m);
    TypeCreation::Initialize(m);
    PyExpression::Initialize(m);
    PyRelation::Initialize(m);
    PyConnection::Initialize(m);
    m.def("connect", &PyConnection::Connect, 
            "Create a OtterBrix database instance. Can take a database file name to read/write persistent data and a "
            "read_only flag if no changes are desired",
            py::arg("database") = "default", py::arg("read_only") = false, py::arg_v("config", py::dict(), "None"));

    // https://pybind11.readthedocs.io/en/stable/advanced/misc.html#module-destructors
    auto clean_default_connection = []() {
        PyConnection::Cleanup();
    };
    m.add_object("_clean_default_connection", py::capsule(clean_default_connection)); 


}
