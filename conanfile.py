from conan import tools, ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.build import check_min_cppstd
from conan.errors import ConanInvalidConfiguration


"""
4. Структура проекта
otterbrix/
├── conanfile.py      # корневой conanfile — требует boost, fmt, ... но не otterbrix
├── CMakeLists.txt    # собирает core, components, services, integration
├── core/
├── components/
├── services/
└── integration/
    ├── c/            # C API
    ├── cpp/          # C++ API
    └── python/       # Python bindings (если BUILD_PYTHON)
"""

"""
У пакета Conan, который хотят использовать через requires("otterbrix/1.0"), обычно есть метод package():
Текущий conanfile только собирает проект, а не готовит его к распространению.
"""


class OtterbrixConan(ConanFile):
    name = "otterbrix"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"

    options = {"build_python": [True, False]}
    default_options = {"build_python": False}

    def configure(self):
        """
        otterbrix собирает всё сам, включая Python-биндинги.
        """
        self.requires("boost/1.87.0", override=True)
        self.requires("fmt/11.1.3@")
        self.requires("spdlog/1.15.1@")
        if self.options.build_python:
            self.requires("pybind11/2.13.6@")
            # utf8proc и tabulate — только для Python (numpy_scan, box_render); CMake ищет их только при BUILD_PYTHON
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


