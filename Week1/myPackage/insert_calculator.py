"""
Module for Interest calculation
"""

def simple_intrest(p, t, r):
    """
    :Desc : Calculating Simple Interest
    :param p: principal amount
    :param t: no of years
    :param r: rate of interest
    :return: SI, total amount
    """
    si = (p * t * r) / 100
    amount = p + si
    return si, amount


def compound_intrest(p, t, r):
    """
    :Desc : Calculating Compound Interest
    :param p: principal amount
    :param t: no of years
    :param r: rate of interest
    :return: Compound Interest, total amount
    """
    amount = p * (1 + (r / 100)) ** t
    return  amount
