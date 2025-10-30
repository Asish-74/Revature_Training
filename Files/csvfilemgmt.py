import csv
import os

def write_csv(filename):
    data = [
        {"name": "John Doe", "age": 30},
        {"name": "Jane Smith", "age": 25}
    ]
    headers = ["name", "age"]
    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

def read_csv(filename):
    with open(filename, "r", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            print(f"Name: {row['name']}, Age: {row['age']}")

# Function to delete a CSV file
def delete_csv(filename):
    if os.path.exists(filename):
        os.remove(filename)
        print(f"{filename} deleted successfully")
    else:
        print(f"{filename} does not exist")


filename = "data.csv"
write_csv(filename)
print("Data read from CSV file:")
read_csv(filename)
delete_csv(filename)


