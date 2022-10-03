#DSC 510
#Week 7 Programming Assignment 7.1
#Programming Assignment Week 7
#Author: Binay P Jena
#07/17/2020

'''
We will create a program which performs three essential operations. It will process this .txt file. Calculate the total words, and output the number of occurrences of each word in the file.

Open the file and process each line.
Either add each word to the dictionary with a frequency of 1 or update the word’s count by 1.
Nicely print the output, in this case from high to low frequency. You should use string formatting for this.
We want to achieve each major goal with a function (one function, one action). We can find four functions that need to be created.

add_word: Add each word to the dictionary. Parameters are the word and a dictionary. No return value.

Process_line: There is some work to be done to process the line: strip off various characters, split out the words, and so on. Parameters are a line and the dictionary. It calls the function add word with each processed word. No return value.

Pretty_print: Because formatted printing can be messy and often particular to each situation (meaning that we might need to modify it later), we separated out the printing function. The parameter is a dictionary. No return value.

main: We will use a main function as the main program. As usual, it will open the file and call process_line on each line. When finished, it will call pretty_print to print the dictionary.

In the main function, you will need to open the file. We will cover more regarding opening of files next week but I wanted to provide you with the block of code you will utilize to open the file, see below
'''

import string
input_txt_file_name = "sample.txt"
output_txt_file_name = "sample_formatted.txt"


def _process_file(dictionary):
    #print(dictionary)
    #open_f = open(output_txt_file_name, 'w')
    #unique_word_count = "Length of the dictionary: {} ".format(str(len(dictionary)))
    #open_f.write(unique_word_count)
    #open_f.write("Word                    Count\n")
    #open_f.write("-----------------------------")
    #print_var = sorted(((word, count) for word, count in dictionary.items()), key=lambda x: x[1], reverse=True)
    #for word, count in print_var:
    #open_f = open(output_txt_file_name, 'w')
    #with open(output_txt_file_name, 'w') as open_f:
     #   open_f.write("{}\t{}".format(word.ljust(20, ' '), count))
      #  open_f.close()
    with open(output_txt_file_name, 'w') as f:
        unique_word_count = "Length of the dictionary: {} \n".format(str(len(dictionary)))
        print(unique_word_count, file=f)
        print("Word                    Count\n")
        print("-----------------------------\n")
        print_var = sorted(((word, count) for word, count in dictionary.items()), key=lambda x: x[1], reverse=True)
        for word, count in print_var:
            print("{}\t{}".format(word.ljust(20, ' '), count), file=f)
        #print(dictionary, file=f)
    read_file = open(output_txt_file_name, 'r')
    file_contents = read_file.read()
    read_file.close()
    print(file_contents)


def _pretty_print(dictionary):
    unique_word_count = "Length of the dictionary: {} ".format(str(len(dictionary)))
    print(unique_word_count)
    print("Word                    Count\n")
    print("-----------------------------")
    print_var = sorted(((word, count) for word, count in dictionary.items()), key=lambda x: x[1], reverse=True)
    for word, count in print_var:
        print("{}\t{}".format(word.ljust(20, ' '), count))


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
    text = open(input_txt_file_name, "r")
    for line in text:
        #print(line)
        _process_line(line=line, dictionary=main_d)
    #print(main_d)
    #_pretty_print(dictionary=main_d)
    _process_file(dictionary=main_d)


if __name__ == '__main__':
    main()