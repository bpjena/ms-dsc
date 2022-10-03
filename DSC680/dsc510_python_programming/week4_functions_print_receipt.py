#DSC 510
#Week 4 Programming Assignment 4.1
#Programming Assignment Week 4
#Author: Binay P Jena
#06/25/2020
'''
Your program will calculate the cost of fiber optic cable installation by multiplying the number of feet needed by $0.87.
We will also evaluate a bulk discount. You will prompt the user for the number of fiber optic cable they need installed.
Using the default value of $0.87 calculate the total expense. If the user purchases more than 100 feet, they will be charged $0.80 per foot.
If the user purchases more than 250 feet, they will be charged $0.70 per foot. If they purchase more than 500 feet, they will be charged $0.50 per foot.

Modify your IF Statement program to add a function. This function will perform the cost calculation. The function will have two parameters (feet and price). When you call the function, you will pass two arguments to the function; feet of fiber to be installed and the cost (remember that price is dependent on the number of feet being installed). You should have the following:
Your program must have a header. Use the SIU Edwardsville Programming Guide for guidance.
A welcome message.
A function with two parameters.
A call to the function.
The application should calculate the cost based upon the number of feet being ordered.
A printed message displaying the company name and the total calculated cost.
All costs should display in USD Currency Format Ex: $123.45.
'''

from datetime import datetime

execution_date = datetime.now().strftime("%Y-%m-%d")

def _total_charges (length, price):
    install_cost = length * _pick_rate(length, price)
    print("Installation Charge  : $ " + str(install_cost))
    misc_cost = 0 #not relevant for Week 4 assignment scope
    addnl_tax = 0 #not relevant for Week 4 assignment scope
    total_charges = install_cost + misc_cost + addnl_tax
    print("Total Charges        : $ " + str(total_charges))

def _pick_rate (length, price):
    if length < 0.0:
        rate = 0.0
    elif length >= 0.0 and length <= 100.0:
        rate = 0.87*price
    elif length > 100.0 and length <= 250.0:
        rate = 0.80*price
    elif length > 250.0 and length <= 500.0:
        rate = 0.70*price
    else:
        rate = 0.50*price
    print("Installation Length  :   " + str(length) + " feet")
    print("Fibre Price per feet : $ " + str(price))
    print("Rate applied         : $ " + str(rate))
    return rate

def main():
    print("Greetings ! Have a nice day !!")
    company = input("Please provide company name: ")
    length_ip = input("Provide installation length in feet (enter digits only w decimals): ")
    price_ip = input("Provide fiber price (enter digits only w decimals): ")
    length = float(length_ip)
    price = float(price_ip)
    print("Hello, We're pleased to serve " + company.capitalize() + ", today " + execution_date + " ... ")
    print("Receipt              : " + "Date " + execution_date)
    print("Transaction Details  : ")
    _total_charges(length=length, price=price)
    print("We look forward to doing business with you again... ")
    print("Thank You !!")

if __name__=='__main__':
    main()