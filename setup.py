import sys

from Cython.Build import cythonize
from setuptools import Extension, setup

CYTHON_COMPILER_DIRECTIVES = {
    "language_level": "3",
    "boundscheck": False,
    "wraparound": False,
    "cdivision": True,
    "initializedcheck": False,
}


def extension_extra_compile_args() -> list[str]:
    if sys.platform == "win32":
        return ["/O2"]
    return ["-O3", "-march=native"]


extensions = [
    Extension(
        "tradepy.strategy.portfolio_alloc",
        ["tradepy/strategy/portfolio_alloc.pyx"],
        extra_compile_args=extension_extra_compile_args(),
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        build_dir="build/cython",
        compiler_directives=CYTHON_COMPILER_DIRECTIVES,
    )
)
