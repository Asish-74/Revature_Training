"""
Banking Interest Calculation
"""
from Week1.myPackage.insert_calculator import *

p = float(input("Enter the Principal: "))
t = float(input("Enter the Time (in years): "))
r = float(input("Enter the Rate (%): "))

si, amt = simple_intrest(p, t, r)
print(f"Simple Interest: {si:.2f}, Total Amount: {amt:.2f}")

ci = compound_intrest(p, t, r)
print(f"Compound Interest: {ci:.2f}")
