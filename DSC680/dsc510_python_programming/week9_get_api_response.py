#DSC 510
#Week 9 Programming Assignment 9.1
#Programming Assignment Week 9
#Author: Binay P Jena
#08/01/2020

'''
We’ve already looked at several examples of API integration from a Python perspective and this week we’re going to write a program that uses an open API to obtain data for the end user.
Create a program which uses the Request library to make a GET request of the following API: Chuck Norris Jokes 'https://api.chucknorris.io/jokes/random'
The program will receive a JSON response which includes various pieces of data. You should parse the JSON data to obtain the “value” key. The data associated with the value key should be displayed for the user (i.e., the joke).
Your program should allow the user to request a Chuck Norris joke as many times as they would like. You should make sure that your program does error checking at this point. If you ask the user to enter “Y” and they enter y, is that ok? Does it fail? If it fails, display a message for the user. There are other ways to handle this. Think about included string functions you might be able to call.
Your program must include a header as in previous weeks.
Your program must include a welcome message for the user.
Your program must generate “pretty” output. Simply dumping a bunch of data to the screen with no context doesn’t represent “pretty.”
'''

import json
import requests
from datetime import datetime

url = 'https://api.chucknorris.io/jokes/random'
execution_date = datetime.now().strftime("%Y-%m-%d")
execution_timestamp = datetime.now()


def _validate_char_input():
    raw_input = input("Enter Y to CONTINUE : ")
    if raw_input.lower() == 'y':
        _get_url_response(url)
        _validate_char_input()
    else:
        print("thank you ! have a good day... ")


def _get_url_response(string_param):
    r = requests.get(string_param)
    entire_response = r.json()
    print("the api attempted access is... \n" + string_param)
    print("\nthe values in the api response are... \n")
    response_id = entire_response['id']
    response_value = entire_response['value']
    response_url = entire_response['url']
    response_icon_url = entire_response['icon_url']
    response_created_at = entire_response['created_at']
    response_updated_at = entire_response['updated_at']
    print("Chuck Norris' trivia: \n" + response_value) #this is 'value' in dict
    print("\nadditional values in this response are... \n")
    print("id : " + response_id)
    print("url: " + response_url)
    print("icon_url: " + response_icon_url)
    print("created_at: " + response_created_at)
    print("updated_at: " + response_updated_at)
    print("executed_at: " + str(execution_timestamp))


def main():
    print("hello there !!! \nhope your day's going great... \n")
    print("mood for some Chuck Norris joke ... ??? \n")
    try:
        _validate_char_input()
        r = requests.get(url, timeout=3)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errH:
        print("HTTP Error: ", errH)
        print("verify url... ")
    except requests.exceptions.ConnectionError as errC:
        print("Connection Error: ", errC)
        print("verify connection... ")
    except requests.exceptions.Timeout as errT:
        print("Timeout Error: ", errT)
        print("try after a while... ")
    except requests.exceptions.RequestException as err:
        print("OOPS !! Something Else Happened... ", err)


if __name__ == "__main__":
    main()