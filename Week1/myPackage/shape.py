"""
Area circumference of SQ, Rect, Tringle
"""
from math import pi
def area_of_square(side):
    """
    Area of square Side
    :param side: Side of square
    :return: the area of square
    """
    return side * side

def area_of_circle (radius):
    """
    Area of circle Side
    :param radius: radius of circle
    :return: area of circle
    """
    return pi*radius**2

def area_of_rectangle (l,b):
    """
    Area of rectangle Side
    :param l: length of rectangle
    :param b: breadth of rectangle
    :return: area of rectangle
    """
    return l*b

def cir_of_circle (radius):
    """
    circumference of circle
    :param radius:  radius of circle
    :return: circumference of circle
    """
    return 2*pi*radius
def cir_of_rectangle (l,b):
    """
    circumference of rectangle
    :param l: length of rectangle
    :param b:  breadth of rectangle
    :return: circumference of rectangle
    """
    return 2*(l+b)
