# setup_session.py
from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension
import os

MOXYGEN_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

ext_modules = [Extension(
    "moxygen.moqsession.session",
    ["moxygen/moqsession/session.pyx"],
    language="c++",
    include_dirs=[MOXYGEN_ROOT, "/usr/local/include"],
    library_dirs=["/usr/local/lib", "/usr/lib"],
    libraries=["folly", "proxygen", "glog", "double-conversion", "fmt", 
              "event", "boost_context", "boost_system"],
    define_macros=[("FOLLY_NO_CONFIG", None), ("FOLLY_HAVE_PTHREAD", 1),
                  ("CYTHON_TRACE", "1")],
    extra_compile_args=["-std=c++17", "-O3", "-Wall", "-Wextra", "-fPIC",
                       "-fprofile-arcs", "-ftest-coverage"],
    extra_link_args=["-fprofile-arcs", "-ftest-coverage"],
)]

setup(
    name="moxygen-moqsession",
    version="0.1",
    description="Python bindings for MoqSession",
    ext_modules=cythonize(ext_modules, build_dir="build",
                         compiler_directives={"language_level": "3",
                                           "linetrace": True,
                                           "profile": True,
                                           "binding": True}),
    packages=["moxygen.moqsession"],
    package_dir={"moxygen.moqsession": "moxygen/moqsession"}
)