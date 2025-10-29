from Opps.AgeNotEnoughError import AgeNotEnoughError
from Opps.Calculations import Calculations

# Taking inputs from the user
n1 = int(input("Enter your first number: "))
n2 = int(input("Enter your second number: "))
age = int(input("Enter your age: "))

# Creating an object of Calculations class
calc = Calculations(n1, n2)

# Displaying results for basic operations
print(f"Addition = {calc.add()}")
print(f"Subtraction = {calc.sub()}")
print(f"Multiplication = {calc.mul()}")
try:
    #  Raise ZeroDivisionError manually if n2 = 0
    if n2 == 0:
        raise ZeroDivisionError("Division by zero")
    #  Raise custom error if age < 18
    if age < 18:
        raise AgeNotEnoughError("You are Minor")
    l1 = [1, 5, 7, 3]
    val = l1[10]
    res = calc.div()
    print(val)
    print(f"Division = {res}")
# Handling specific exceptions
except ZeroDivisionError as e:
    print(f"❌ Error: {e}")
except IndexError:
    print("❌ Error: Index not available in the list.")
except AgeNotEnoughError as e:
    print(f"❌ Age Error: {e}")
except Exception as e:
    # Catching any other unknown error
    print(f"❌ Something went wrong: {e} ")
else:
    # Executes only if no exception occurs
    print("✅ All operations executed successfully!")
finally:
    # Always executes whether exception occurs or not
    print("✅ Program execution completed.")
