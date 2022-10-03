#DSC 510
#Week 2 Programming Assignment 1.2
#Programming Assignment Week 2
#Author: Binay P Jena
#06/12/2020
from datetime import datetime

execution_date = datetime.now().strftime("%Y-%m-%d")

def _total_charges (length):
    print("Transaction Details - ")
    install_cost = length * 0.87
    print("Installation Length : " + str(length) + " feet")
    print("Installation Charge : $ " + str(install_cost))
    misc_cost = 0 #not relevant for Week 2 assignment scope
    addnl_tax = 0 #not relevant for Week 2 assignment scope
    total_charges = install_cost + misc_cost + addnl_tax
    print("Total Charges : $ " + str(total_charges))



print("Greetings ! Have a nice day !!")
company = input("Please provide company name: ")
length_ip = input("Provide installation length in feet (enter digits only w decimals): ")
length = float(length_ip)
print("Hello, We're pleased to serve " + company + ", today " + execution_date + " ... ")
print("Receipt : ")
print("Date : " + execution_date)
_total_charges(length=length)
print("We look forward to doing business with you again... ")
print("Thank You !!")