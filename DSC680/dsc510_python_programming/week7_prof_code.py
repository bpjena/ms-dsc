'''
Last week we got a taste of working with files. This week we’ll really dive into files by opening and closing files properly.

For this week we will modify our Gettysburg processing program from week 7 in order to generate a text file from the output rather than printing to the screen. Your program should have a new function called process_file which prints to the file. (This method should almost be the same as the pretty_print function from last week.) Keep in mind that we have print statements in main as well. Your program must modify the print statements from main as well.

Your program must have a header. Use the programming style guide for guidance.
Create a new function called process_fie. This function will perform the same operations as pretty_print from week 7 however it will print to a file instead of to the screen.
Modify your main method to print the length of the dictionary to the file as opposed to the screen.
This will require that you open the file twice. Once in main and once in process_file.
Prompt the user for the filename they wish to use to generate the report.
Use the filename specified by the user to write the file.
This will require you to pass the file as an additional parameter to your new process_file function.
'''

import string


def process_line(line, word_count_dict):
    """Process the line to get lowercase words to add to the dictionary. """
    line = line.strip()
    print(type(line))
    word_list = line.split()
    print(type(word_list))
    for word in word_list:
        # ignore the '−−' that is in the file
        if word != '--':
            word = word.lower()
            word = word.strip()
            # get commas, periods, and other punctuation out as well
            word = word.strip(string.punctuation)
            add_word(word, word_count_dict)


def add_word(word, word_count_dict):
    """Update the word frequency: word is the key, frequency is the value """
    if word in word_count_dict:
        word_count_dict[word] += 1
    else:
        word_count_dict[word] = 1


def pretty_print(word_count_dict):
    """Print nicely from highest to lowest frequency """
    value_key_list = []
    for key, val in word_count_dict.items():
        value_key_list.append((val, key))
    # sort method sorts on list's first element, the frequency.
    # Reverse to get biggest first
    value_key_list.sort(reverse=True)
    # value_key_list = sorted([v,k) for k, v in value_key_list.items()]
    print('{:11s}{:11s}'.format('Word', 'Count'))
    print(' ' * 21)
    for val, key in value_key_list:
        print('{:12s} {:<3d}'.format(key, val))


def main():
    word_count_dict = {}
    try:
        gba_file = open('gettysburg.txt', 'r')
    except FileNotFoundError as e:
        print(e)
    for line in gba_file:
        process_line(line, word_count_dict)
    print(word_count_dict)
    print('Length of the dictionary:', len(word_count_dict))
    pretty_print(word_count_dict)


if __name__ == "__main__":
    # execute only if run as a script
    main()