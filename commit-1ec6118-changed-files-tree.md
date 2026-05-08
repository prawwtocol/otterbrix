# Дерево изменённых файлов

**Коммит:** `1ec611842591` (`1ec6118`)

**Заголовок:** feat: Adapt pythonpkg sources to otterbrix build system and APIs

**Дата:** 2026-03-19 19:50:00 +0300

**Автор:** seliverstow <seliverstow@yandex-team.ru>

Легенда статусов: **M** — изменён, **A** — добавлен, **D** — удалён, **R** — переименован/перемещён (в строке указан исходный путь для **R**).

---

## Дерево путей

```text
core/
  CMakeLists.txt  M
integration/
  python/
    arrow/
      arrow_array_stream.cpp  M
    components/
      arrow/
        appender/
          fixed_size_list_data.cpp  M
          map_data.hpp  M
        arrow_converter.cpp  M
      function/
        table_function.hpp  M
    connection_environment/
      expression/
        expression_factory.cpp  M
        expression_factory.hpp  M
      relation/
        relation.cpp  M
      connection_environment.cpp  M
    native/
      python_conversion.cpp  M
      python_objects.cpp  M
      python_objects.hpp  M
    numpy/
      array_wrapper.cpp  M
    otterbrix/
      experimental/
        spark/
          sql/
            column.py  M
            dataframe.py  M
            functions.py  M
            group.py  M
            logical_plan.py  A
            optimizer.py  A
            session.py  M
      __init__.py  M
    otterbrix_wrapper/
      pyexpression.cpp  M
      pyexpression.hpp  M
      pyrelation.cpp  M
      pyresult.cpp  M
      pytype.cpp  M
      type_creation.cpp  M
    pandas/
      analyzer.cpp  M
    scan/
      python_replacement_scan.cpp  M
    tests/
      cpp/
        native/
          CMakeLists.txt  D
          main.cpp  D
          test_hello.cpp  D
        CMakeLists.txt  D
      fast/
        spark/
          test_logical_plan.py  A
          test_optimizer.py  A
          test_spark_catalog.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_catalog.py
          test_spark_dataframe_sort.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_dataframe_sort.py
          test_spark_filter.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_filter.py
          test_spark_group_by.py  R095  ←  integration/python/tests/python/fast/spark/test_spark_group_by.py
          test_spark_join.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_join.py
          test_spark_order_by.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_order_by.py
          test_spark_pandas_dataframe.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_pandas_dataframe.py
          test_spark_types.py  R100  ←  integration/python/tests/python/fast/spark/test_spark_types.py
      conftest.py  R069  ←  integration/python/tests/python/conftest.py
    util/
      box_render.cpp  M
      convert_value.cpp  M
      convert_value.hpp  M
      util.cpp  M
      util.hpp  M
    CMakeLists.txt  M
    main.cpp  M
    main2.cpp  D
    main3.cpp  M
    pyutil.hpp  M
    pywrapper.py  M
  CMakeLists.txt  M
.gitignore  M
CMakeLists.txt  M
CMakeUserPresets.json  A
conanfile.py  M
MANIFEST.in  D
pyproject.toml  D
setup.py  D
```

---

*Сгенерировано из `git show 1ec6118 --name-status`.*
