#DSC 510
#Week 8 Programming Assignment 8.1
#Programming Assignment Week 8
#Author: Binay P Jena
#07/26/2020

'''
Last week we got a taste of working with files. This week we’ll really dive into files by opening and closing files properly.

For this week we will modify our Gettysburg processing program from week 7 in order to generate a text file from the output rather than printing to the screen. Your program should have a new function called process_file which prints to the file. (This method should almost be the same as the pretty_print function from last week.) Keep in mind that we have print statements in main as well. Your program must modify the print statements from main as well.

Your program must have a header. Use the programming style guide for guidance.
Create a new function called process_file. This function will perform the same operations as pretty_print from week 7 however it will print to a file instead of to the screen.
Modify your main method to print the length of the dictionary to the file as opposed to the screen.
This will require that you open the file twice. Once in main and once in process_file.
Prompt the user for the filename they wish to use to generate the report.
Use the filename specified by the user to write the file.
This will require you to pass the file as an additional parameter to your new process_file function.
'''

import string
input_txt_file_name = "gettysburg.txt"


def _process_file(dictionary):
    output_file_name_ = input("Enter output file name : ")
    output_txt_file_name = output_file_name_ + ".txt"
    with open(output_txt_file_name, 'w') as f:
        unique_word_count = "Length of the dictionary: {} \n".format(str(len(dictionary)))
        print(unique_word_count, file=f)
        print("Word                    Count\n", file=f)
        print("-----------------------------\n", file=f)
        print_var = sorted(((word, count) for word, count in dictionary.items()), key=lambda x: x[1], reverse=True)
        for word, count in print_var:
            print("{}\t{}".format(word.ljust(20, ' '), count), file=f)
    print("completed write-to-output file : " + output_txt_file_name)
    read_file = open(output_txt_file_name, 'r')
    file_contents = read_file.read()
    read_file.close()
    #print(file_contents) #check: prints file-contents on screen


def _add_words(words, dictionary):
    d = dictionary
    # iterate over each word in words
    for word in words:
        # check if word is already in dict
        if word in d:
            # increment word count by 1
            d[word] = d[word] + 1
        else:
            # count as first word
            d[word] = 1


def _process_line(line, dictionary):
    # remove leading spaces and newline chars
    line = line.strip()

    # convert chars to lower to avoid case mismatch
    line = line.lower()

    # remove punctuation marks
    line = line.translate(line.maketrans('', '', string.punctuation))

    # split line to words
    words = line.split(" ")
    _add_words(words=words, dictionary=dictionary)


def main():
    main_d = dict()
    try:
        text = open(input_txt_file_name, "r")
    except FileNotFoundError as e:
        print(e)
    for line in text:
        _process_line(line=line, dictionary=main_d)
    _process_file(dictionary=main_d)


if __name__ == '__main__':
    main()