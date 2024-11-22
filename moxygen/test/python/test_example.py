import pytest
from cy_example import PyCalculator

def test_calculator_add():
    calc = PyCalculator()
    assert calc.add(2, 3) == 5
    assert calc.add(-1, 1) == 0

def test_calculator_multiply():
    calc = PyCalculator()
    assert calc.multiply(2.0, 3.0) == 6.0
    assert calc.multiply(-2.0, 3.0) == -6.0

@pytest.mark.parametrize("a,b,expected", [
    (2, 3, 5),
    (-1, 1, 0),
    (0, 0, 0)
])
def test_calculator_add_parametrized(a, b, expected):
    calc = PyCalculator()
    assert calc.add(a, b) == expected