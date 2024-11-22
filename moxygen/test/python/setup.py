from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension

ext_modules = [
    Extension(
        "cy_example",
        ["cy_example.pyx", "example.cpp"],
        language="c++",
        include_dirs=["."],
        define_macros=[('CYTHON_TRACE', '1')],
        extra_compile_args=['-fprofile-arcs', '-ftest-coverage'],  # Enable gcov
        extra_link_args=['-fprofile-arcs', '-ftest-coverage']
    )
]

setup(
    ext_modules=cythonize(
        ext_modules, 
        build_dir='build',
        compiler_directives={
            'linetrace': True,
            'profile': True
        }
    )
)