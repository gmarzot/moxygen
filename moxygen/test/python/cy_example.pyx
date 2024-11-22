# cython: binding=True
# cython: linetrace=True
# cython: profile=True
# cython: language_level=3
# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# distutils: define_macros=CYTHON_TRACE=1
# distutils: language = c++

cdef extern from "example.hpp":
    cdef cppclass Calculator:
        Calculator() except +
        int add(int a, int b)
        double multiply(double a, double b)

cdef class PyCalculator:
    cdef Calculator* c_calc

    def __cinit__(self):
        self.c_calc = new Calculator()
    
    def __dealloc__(self):
        del self.c_calc
    
    def add(self, int a, int b):
        return self.c_calc.add(a, b)
    
    def multiply(self, double a, double b):
        return self.c_calc.multiply(a, b)