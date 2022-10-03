#DSC 510
#Week 4 Programming Assignment 5.1
#Programming Assignment Week 5
#Author: Binay P Jena
#07/04/2020
'''
Your program must have a header.
This program will perform various calculations (addition, subtraction, multiplication, division, and average calculation).
This program will contain a variety of loops and functions.
The program will add, subtract, multiply, divide two numbers and provide the average of multiple numbers input by the user.
Define a function named performCalculation which takes one parameter. The parameter will be the operation being performed (+, -, *, /).
This function will perform the given prompt the user for two numbers then perform the expected operation depending on the parameter that's passed into the function.
This function will print the calculated value for the end user.
Define a function named calculateAverage which takes no parameters.
This function will ask the user how many numbers they wish to input.
This function will use the number of times to run the program within a for loop in order to calculate the total and average.
This function will print the calculated average.
This program will have a main section which contains a while loop. The while loop will be used to allow the user to run the program until they enter a value which ends the loop.
The main program should prompt the user for the operation they wish to perform.
The main program should evaluate the entered data using if statements.
The main program should call the necessary function to perform the calculation.
'''

from datetime import datetime
execution_date = datetime.now().strftime("%Y-%m-%d")


def _performCalculation(operation):
    a = input("input your first number  ")
    b = input("input your second number  ")
    if str(operation).lower() == '1': #addition
        c = float(a) + float(b)
        return str(c)
    elif str(operation).lower() == '2': #subtraction
        c = float(a) - float(b)
        return str(c)
    elif str(operation).lower() == '3': #multiplication
        c = float(a) * float(b)
        return str(c)
    elif str(operation).lower() == '4': #division
        c = float(a) / float(b)
        return str(c)
    else:
        return "invalid operation"

def _calculateAverage():
    numbers = int(input("how many numbers would you like to average ? "))
    total_sum = 0
    for n in range(numbers):
        nums = float(input('Enter number : '))
        total_sum += nums
    avg = total_sum/numbers
    return str(avg)

def _name_operation(number):
    if str(number) == '1':
        return 'addition'
    elif str(number) == '2':
        return 'subtraction'
    elif str(number) == '3':
        return 'multiplication'
    elif str(number) == '4':
        return 'division'
    elif str(number) == '5':
        return 'average'
    else:
        return 'invalid operation'

def _eval_response():
    prompt = input("do you want to continue y/n ? ")
    if str(prompt) == 'y':
        main()
    elif str(prompt) == 'n':
        pass
    else:
        print("invalid input")

def main():
    ops_input = str(input("would you like to add, subtract, multiply, divide, or calculate average ? "
                          "[Enter 1 to add, 2 to subtract, 3 to multiply, 4 to divide, or 5 for average]"))
    if ops_input == '5':
        c = _calculateAverage()
    elif ops_input in ['1', '2', '3', '4']:
        c = _performCalculation(ops_input)
    else:
        print("invalid input")
    print("the calculated value after " + _name_operation(ops_input) + " operation is : " + c)
    _eval_response()

if __name__ == "__main__":
    main()
