#DSC 510
#Week 3 Programming Assignment 3.1
#Programming Assignment Week 3
#Author: Binay P Jena
#06/17/2020
'''
Your program will calculate the cost of fiber optic cable installation by multiplying the number of feet needed by $0.87.
We will also evaluate a bulk discount. You will prompt the user for the number of fiber optic cable they need installed.
Using the default value of $0.87 calculate the total expense. If the user purchases more than 100 feet, they will be charged $0.80 per foot.
If the user purchases more than 250 feet, they will be charged $0.70 per foot. If they purchase more than 500 feet, they will be charged $0.50 per foot.
#1# Display a welcome message for your program.
#2# Get the company name from the user.
#3# Get the number of feet of fiber optic cable to be installed from the user.
#4# Evaluate the total cost based upon the number of feet requested.
#5# Display the calculated information including the number of feet requested and company name.
'''

from datetime import datetime

execution_date = datetime.now().strftime("%Y-%m-%d")

def _total_charges (length):
    print("Transaction Details - ")
    install_cost = length * _pick_rate(length)
    print("Installation Length : " + str(length) + " feet")
    print("Installation Charge : $ " + str(install_cost))
    misc_cost = 0 #not relevant for Week 3 assignment scope
    addnl_tax = 0 #not relevant for Week 3 assignment scope
    total_charges = install_cost + misc_cost + addnl_tax
    print("Total Charges : $ " + str(total_charges))

def _pick_rate (length):
    if length < 0.0:
        rate = 0.0
    elif length >= 0.0 and length <= 100.0:
        rate = 0.87
    elif length > 100.0 and length <= 250.0:
        rate = 0.80
    elif length > 250.0 and length <= 500.0:
        rate = 0.70
    else:
        rate = 0.50
    return rate

print("Greetings ! Have a nice day !!")
company = input("Please provide company name: ")
length_ip = input("Provide installation length in feet (enter digits only w decimals): ")
length = float(length_ip)
print("Hello, We're pleased to serve " + company.capitalize() + ", today " + execution_date + " ... ")
print("Receipt : ")
print("Date : " + execution_date)
_total_charges(length=length)
print("We look forward to doing business with you again... ")
print("Thank You !!")