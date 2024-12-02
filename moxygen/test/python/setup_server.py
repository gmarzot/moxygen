# setup_server.py
from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension

ext_modules = [
    Extension(
        "moxygen.moqserver",
        ["moxygen/moqserver/server.pyx"],
        language="c++",
        include_dirs=["."],
        define_macros=[('CYTHON_TRACE', '1')],
        extra_compile_args=['-fprofile-arcs', '-ftest-coverage'],
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