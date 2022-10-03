#DSC 510
#Week 10 Programming Assignment 10.1
#Programming Assignment Week 10
#Author: Binay P Jena
#08/07/2020
'''
This week we’re going to demonstrate our knowledge of Python object oriented programming concepts by creating a simple cash register program.

Your program must have a header.
Your program must have a welcome message for the user.
Your program must have one class called CashRegister.
Your program will have an instance method called addItem which takes one parameter for price. The method should also keep track of the number of items in your cart.
Your program should have two getter methods.
getTotal – returns totalPrice
getCount – returns the itemCount of the cart
Your program must create an instance of the CashRegister class.
Your program should have a loop which allows the user to continue to add items to the cart until they request to quit.
Your program should print the total number of items in the cart.
Your program should print the total $ amount of the cart.
The output should be formatted as currency. Be sure to investigate the locale class. You will need to call locale.setlocale and locale.currency.
'''

from datetime import datetime
import locale

# Using SetLocale module to format numbers as currency ($)
locale.setlocale(locale.LC_ALL, 'en_US')
execution_date = datetime.now().strftime("%Y-%m-%d")
execution_timestamp = datetime.now()


class CashRegister(object):
    def __init__(self):
        self.total_price = 0
        self.item_count = 0
    # Instance method - addItem  : Keep Total items and Total price in the Cart

    def addItem(self, price):
        self.total_price += int(price)
        self.item_count += 1

    def getTotal(self):
        return self.total_price

    def getCount(self):
        return self.item_count


def register_and_print():
    register = CashRegister()
    select_option = True
    # loop - allow users to enter the details until they enter N or X
    while select_option:
        select_option = input("Would you like to add another item to the cart Y or N\n")
        # if Y, keep adding items and price into cart
        if select_option.lower() == "y":
            price = input("What is the price of the item?\n")
            register.addItem(price)
        # if N or X, print the total items and price
        elif select_option.lower() == 'n' or select_option.lower() == "x":
            TotalValue = register.getTotal()
            print("The Summary of Cart Details is as below")
            print("#######################################")
            print("Total Number of Items in cart : " + str(register.getCount()))
            print("Total Amount of Items in cart : " + locale.currency(TotalValue, grouping=True))
            select_option = False
            print("thank you ! it was nice to serve you... have a good day... ")
        # if not- Y, N, X, we will ask users to type the correct option
        else:
            print(
                "Invalid Input. Enter Y to add new item into cart, N for total or X to exit")
            select_option = True


def main():
    print("Welcome to the BINAY's Cart Add Experience !")
    print('Date : ' + str(execution_timestamp))  # printing date & time
    print("------------------------------------------")
    register_and_print()


if __name__ == '__main__':
    main()