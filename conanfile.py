from conan import tools, ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.build import check_min_cppstd
from conan.errors import ConanInvalidConfiguration
class OtterbrixConan(ConanFile):
    name = "otterbrix"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"
    # todo надо ли options = {
    #         "shared": [True, False],
    #         "fPIC": [True, False],
    #     }
    options = {"build_python": [True, False]}
    default_options = {"build_python": False}

    def configure(self):
        self.requires("boost/1.87.0", override=True)
        self.requires("fmt/11.1.3@")
        self.requires("spdlog/1.15.1@")
        if self.options.build_python:
            self.requires("pybind11/2.13.6@")

            # todo правильно ли что сюда? мне кажется что pythonpkg если будет собираться то только если self.options.build_python
            self.requires("utf8proc/2.9.0")
            self.requires("tabulate/1.5")
        self.requires("msgpack-cxx/4.1.1@")
        self.requires("catch2/2.13.7@")
        self.requires("abseil/20230802.1@")
        self.requires("benchmark/1.6.1@")
        self.requires("zlib/1.3.1@")
        self.requires("bzip2/1.0.8@")
        self.requires("magic_enum/0.8.1@")
        self.requires("actor-zeta/1.1.1@")

    # options = {
    #     "actor-zeta/*:cxx_standard": [17],
    #     "actor-zeta/*:fPIC": [True, False],
    #     "actor-zeta/*:exceptions_disable": [True, False],
    #     "actor-zeta/*:rtti_disable": [True, False],
    #     # "OpenSSL/*:shared": [True, False],  # commented
    # }
    # default_options = {
    #     "actor-zeta/*:cxx_standard": 17,
    #     "actor-zeta/*:fPIC": True,
    #     "actor-zeta/*:exceptions_disable": False,
    #     "actor-zeta/*:rtti_disable": False,
    #     #"OpenSSL/*:shared": True,
    # }

    def config_options(self):
        if self.settings.get_safe("compiler.cppstd") is None:
            self.settings.cppstd = 20
        self.options["actor-zeta/*"].cxx_standard = 20
        self.options["actor-zeta/*"].fPIC = True
        self.options["actor-zeta/*"].exceptions_disable = False
        self.options["actor-zeta/*"].rtti_disable = False
        self.options["boost/*"].header_only = True


    def validate(self):
        # C++20 required - validated by CMakeLists.txt
        pass

    def layout(self):
        cmake_layout(self)

    def imports(self):
        self.copy("*.so*", dst="build_tools", src="lib")

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "20"
        tc.variables["BUILD_PYTHON"] = bool(self.options.build_python)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()


"""
otterbrix/conanfile.py — что это и зачем нужно
Это файл конфигурации пакетного менеджера Conan для C/C++ проекта otterbrix.

Что делает этот файл
Он описывает все внешние зависимости проекта и настройки сборки. По сути, это аналог package.json (Node.js) или requirements.txt (Python), но для C++.

Разбор по частям
Секция	Назначение
name/version (стр. 6-7)	Имя пакета otterbrix, версия 1.0
settings (стр. 8)	Параметры окружения: ОС, компилятор, тип сборки (Debug/Release), архитектура
options (стр. 9-10)	Опциональная сборка Python-биндингов (build_python, по умолчанию False)
configure() (стр. 12-25)	Список зависимостей — самая важная часть
config_options() (стр. 42-49)	Тонкая настройка опций зависимостей (стандарт C++20, fPIC, boost header-only и т.д.)
generate() (стр. 62-69)	Генерация CMake toolchain-файла и файлов зависимостей для интеграции с CMake
build() (стр. 71-74)	Запуск CMake configure + build
Зависимости проекта
boost/1.87.0 — набор библиотек общего назначения (header-only режим)
fmt/11.1.3 — форматирование строк
spdlog/1.15.1 — логирование
pybind11/2.13.6 — биндинги для Python (опционально)
msgpack-cxx/4.1.1 — сериализация (формат MessagePack)
catch2/2.13.7 — фреймворк для тестирования
abseil/20230802.1 — библиотека утилит от Google
benchmark/1.6.1 — бенчмарки (Google Benchmark)
zlib/1.3.1 и bzip2/1.0.8 — сжатие данных
magic_enum/0.8.1 — рефлексия для enum'ов
actor-zeta/1.1.1 — фреймворк акторной модели
Зачем нужен
Без этого файла невозможно собрать проект, потому что:

Автоматически скачивает и собирает все 12 зависимостей нужных версий
Генерирует CMake-файлы, чтобы CMake знал, где искать заголовки и библиотеки
Обеспечивает воспроизводимость — любой разработчик получит одинаковое окружение, выполнив conan install

"""



"""
pybind11 — это библиотека, которая позволяет вызывать C++ код из Python (и наоборот). Она создаёт так называемые "биндинги" (bindings) — прослойку между двумя языками.

Что это значит в контексте otterbrix
Проект otterbrix написан на C++. Но с помощью pybind11 можно сделать так, чтобы его функциональность была доступна как Python-модуль. То есть пользователь сможет писать:


python

Copy

import otterbrix
# и работать с движком прямо из Python
Почему "опционально"
В conanfile.py:9-10 объявлена опция:


python

Copy

options = {"build_python": [True, False]}
default_options = {"build_python": False}
А в conanfile.py:16-17 зависимость подключается условно:


python

Copy

if self.options.build_python:
    self.requires("pybind11/2.13.6@")
По умолчанию build_python = False, то есть Python-биндинги не собираются. Если кому-то нужен Python-интерфейс, он явно передаёт флаг при установке зависимостей:


bash

Copy

conan install . -o build_python=True
Тогда pybind11 скачается, и CMake соберёт дополнительный Python-модуль (строка 65):


python

Copy

tc.variables["BUILD_PYTHON"] = bool(self.options.build_python)
Итого
Без этой опции otterbrix — чисто C++ проект. С ней — появляется Python API поверх того же C++ ядра.

"""



"""

Actor-zeta — это библиотека, реализующая акторную модель (actor model) — паттерн конкурентного/параллельного программирования. Давайте разберём, зачем это нужно конкретно в otterbrix.

Что такое акторная модель
Акторы — это изолированные объекты, которые:

Имеют собственное состояние (недоступное извне)
Общаются только через сообщения (не через shared memory)
Обрабатывают сообщения последовательно (нет проблем с блокировками и гонками данных)
Это альтернатива классическому подходу с потоками + мьютексами.




Архитектура
Движок разбит на 5 изолированных сервисов, каждый из которых — актор (или группа акторов):


Copy

         wrapper_dispatcher_t     (API фасад)
                  │
         manager_dispatcher_t     (координатор)
           ┌──────┼──────┐
           │      │      │
      executor_t (пул воркеров — выполнение запросов)
           │      │      │
     ┌─────┘      │      └─────┐
     ▼            ▼            ▼
manager_wal_t  manager_disk_t  manager_index_t
  (WAL лог)   (хранилище)     (индексы)
     │            │            │
     ▼            ▼            ▼
  воркеры      воркеры      воркеры
Сервис	Роль
Dispatcher	Принимает SQL-запросы, координирует выполнение
Executor	Пул воркеров, выполняющих физические планы запросов
Disk	Персистентное хранение данных
WAL	Write-Ahead Log — журнал для надёжности записей
Index	Управление индексами (B-tree)


Почему именно акторная модель
Для базы данных это даёт три ключевых преимущества:

Нет мьютексов и гонок данных — каждый сервис изолирован, общение только через сообщения. Это критично, когда десятки потоков одновременно читают и пишут данные.

Параллелизм без сложности — три отдельных пула потоков (для диспетчера, диска и общих задач, по 3 потока каждый) работают кооперативно через корутины C++20. Акторы используют co_await для неблокирующего ожидания результатов от других акторов.

Модульность и подменяемость — каждый сервис реализует «контракт» (интерфейс). Например, есть manager_disk_t для реального диска и manager_disk_empty_t — пустая заглушка. Аналогично для WAL. Можно запустить движок без персистентности, просто подставив пустую реализацию.

Как это выглядит в коде
Актор объявляет свои методы-обработчики через dispatch_traits, а в behavior() (корутине) маршрутизирует входящие сообщения:

Отправка: actor_zeta::send(address, &Method, args...) → возвращает unique_future<T>
Приём: co_await actor_zeta::dispatch(this, &method, msg) внутри behavior()
По сути, actor-zeta — это скелет, на котором держится вся многопоточная архитектура otterbrix.

"""