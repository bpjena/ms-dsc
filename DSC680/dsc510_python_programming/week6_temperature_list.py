#DSC 510
#Week 6 Programming Assignment 6.1
#Programming Assignment Week 6
#Author: Binay P Jena
#07/11/2020

'''
This week we will create a program which works with lists. Your goal is to create a program which contains a list of temperatures.
Your program will populate the list based upon user input.
Your program will determine the number of temperatures in the program, determine the largest temperature, and the smallest temperature.

Create an empty list called temperatures.
Allow the user to input a series of temperatures along with a sentinel value which will stop the user input.
Evaluate the temperature list to determine the largest and smallest temperature.
Print the largest temperature.
Print the smallest temperature.
Print a message informs the user how many temperature readings are in the list.
'''

'''
program stops prompting for input for 0 as sentinel value
'''


def main():
    numbers = []
    while True:
        try:
            raw_input_1 = input("Enter TEMPERATURE [ ENTER 0 TO END ] : ")
            user_input = float(raw_input_1)
            if user_input == 0:
                break
            else:
                numbers.append(user_input)
                raw_input_1
                print("List of Temperatures input are : " + str(numbers))
                print("Largest reading in the list is {}, smallest is {}".format(max(numbers), min(numbers)))
                print("Count of valid readings for Temperature list: " + str(len(numbers)))
        except ValueError:
            print("Not a number, try again!")
            continue


if __name__ == "__main__":
    main()