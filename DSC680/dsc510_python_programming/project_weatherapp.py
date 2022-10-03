#DSC 510
#Course: Introduction To Programming
#Final Project Assignment
# #Week-10
#Author: Binay P Jena
#08/05/2020
'''
Your program must prompt the user for their city or zip code and request weather forecast data from OpenWeatherMap. Your program must display the weather information in a READABLE format to the user.

Requirements:

Create a header for your program just as you have in the past.
Create a Python Application which asks the user for their zip code or city.
Use the zip code or city name in order to obtain weather forecast data from OpenWeatherMap.
Display the weather forecast in a readable format to the user.
Use comments within the application where appropriate in order to document what the program is doing.
Use functions including a main function.
Allow the user to run the program multiple times to allow them to look up weather conditions for multiple locations.
Validate whether the user entered valid data. If valid data isn’t presented notify the user.
Use the Requests library in order to request data from the webservice.
Use Try blocks to ensure that your request was successful. If the connection was not successful display a message to the user.
Use Python 3.
Use try blocks when establishing connections to the webservice. You must print a message to the user indicating whether or not the connection was successful.
'''

import requests
import json
from datetime import datetime

execution_date = datetime.now().strftime("%Y-%m-%d")
execution_timestamp = datetime.now()


def display_app_header():  #this is for fun/info, no intention of disrupting user's flow of actions
    """
    Compiled By: Binay P Jena
    Institute: Bellevue University
    Assignment: Weather App - USA
    Course: DSC 510
    Date: 05 AUG 2020
    Interpeter: Python 3.7
    """


def _pretty_print(dict_input):
    '''
    access dictionary elements and print custom text display on screen
    :param dict_input: dictionary response from api
    :return: none
    '''

    longitude = str(dict_input['coord']['lon'])
    latitude = str(dict_input['coord']['lat'])

    id = str(dict_input['weather'][0]['id'])
    main = str(dict_input['weather'][0]['main'])
    description = str(dict_input['weather'][0]['description'])
    icon = str(dict_input['weather'][0]['icon'])

    temp = str(dict_input['main']['temp'])
    feels_like = str(dict_input['main']['feels_like'])
    temp_min = str(dict_input['main']['temp_min'])
    temp_max = str(dict_input['main']['temp_max'])
    pressure = str(dict_input['main']['pressure'])
    humidity = str(dict_input['main']['humidity'])

    visibility = str(dict_input['visibility'])

    speed = str(dict_input['wind']['speed'])
    deg = str(dict_input['wind']['deg'])
    clouds = str(dict_input['clouds']['all'])

    timestamp = str(datetime.fromtimestamp(dict_input['dt']).strftime('%Y-%m-%d %H:%M:%S'))

    country = str(dict_input['sys']['country'])
    sunrise = str(datetime.fromtimestamp(dict_input['sys']['sunrise']).strftime('%Y-%m-%d %H:%M:%S'))
    sunset = str(datetime.fromtimestamp(dict_input['sys']['sunset']).strftime('%Y-%m-%d %H:%M:%S'))
    city_name = str(dict_input['name'])

    print("\nHere's weather details for " + city_name + ", " + country + " at " + timestamp)
    print("---------------------------------------------------------------------------------------------------\n")
    print("Weather Summary        : " + description)
    print("Weather Highlight      : " + main)
    print("Temperature            : " + temp + " -deg F")
    print("Feels like             : " + feels_like + " -deg F")
    print("Min Temperature        : " + temp_min + " -deg F")
    print("Max Temperature        : " + temp_max + " -deg F")
    print("Pressure               : " + pressure + " hPa")
    print("Humidity               : " + humidity + " %")
    print("Visibility             : " + visibility + " feet")
    print("Wind Speed             : " + speed + " miles/hour")
    print("Wind Direction         : " + deg + " meteorological degrees")
    print("Cloudiness             : " + clouds + " %")
    print(city_name + " addnl stats... ")
    print("Latitude               : " + latitude)
    print("Longitude              : " + longitude)
    print("Last Sunrise timestamp : " + sunrise)
    print("Last Sunset timestamp  : " + sunset + "\n")


def _access_url(decoded_input):
    '''
    input is variable string sequence in the url based on user input criteria selection
    :param decoded_input: based on user's city or zipcode selection
    :return: none
    '''
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    app_id = "9d73187ffe854e314522eba01d58ba0e"
    url = base_url + decoded_input + "&APPID=" + app_id + "&units=imperial"
    _get_url_response(string_param=url)
    _prompt_to_continue()


def _get_url_response(string_param):
    '''
    gets the api response as a dictionary object
    :param string_param: api url input
    :return: none
    '''
    try:
        r = requests.get(string_param, timeout=3)
        r.raise_for_status()
        print('\nConnection Successful!')
        entire_response = r.json()
        _pretty_print(dict_input=entire_response)
    except requests.exceptions.HTTPError as errH:
        print("HTTP Error: ", errH)
        print("verify url... " + string_param)
    except requests.exceptions.ConnectionError as errC:
        print("Connection Error: ", errC)
        print("verify connection... ")
    except requests.exceptions.Timeout as errT:
        print("Timeout Error: ", errT)
        print("try after a while... ")
    except requests.exceptions.RequestException as err:
        print("OOPS !! Something Else Happened... ", err)


def _prompt_to_continue():
    '''
    ask user to continue, start over w choice ask prompt to user
    :return: none
    '''
    ask_continue = input("Want to try once more ? ENTER Y to continue : ") #any entry other than Y QUITs program
    if ask_continue.lower() == 'y':
        get_user_input_criteria()
    else:
        print("thank you ! have a good day... ")


def verify_city_zip(entry, opt):
    '''
    for cities, anything not-a-letter-or-space would be invalid entry, for zipcode only 5-digit numbers are valid
    prompt the user again for corrected entry in case of invalid entries
    :param entry: string [city or zipcode]
    :param opt: string [expected to be 1-char string value as 'c' or 'z']
    :return: True OR False
    '''
    if opt == 'c':  # Entry is a city name
        cn = entry
        for each_letter in cn:
            if not (str(each_letter).isalpha() or str(each_letter).isspace()): #letters and space only
                print('City should have only string or space as entries... invalid entry= ' + entry + '  TRY AGAIN ? \n')
                return False
        return True
    elif opt == 'z':  # Entry is a zip code
        zip_code = entry
        if len(zip_code) != 5:
            print('Zip code should be 5 char only... invalid entry= ' + entry + '  TRY AGAIN ? \n')
            return False
        else:
            for each_letter in zip_code:
                if not str(each_letter).isnumeric():  # only numbers
                    print('Zip code should have only numbers... invalid entry= ' + entry + '  TRY AGAIN ? \n')
                    return False
        return True


def _get_zipcode_weather(input_val):
    '''
    set of actions to execute for user's zipcode criteria selection
    :param input_val: validated user choice as Z
    :return: none
    '''
    zip_input = input("Enter Zipcode: ")
    if verify_city_zip(entry=zip_input, opt=input_val):
        zip_input_url = "zip=" + str(zip_input)
        _access_url(decoded_input=zip_input_url)
    else:
        get_user_input_criteria()


def _get_city_weather(input_val):
    '''
    set of actions to execute for user's city criteria selection
    :param input_val: validated user choice as C
    :return: none
    '''
    city_input = input("Enter City name: ")
    if verify_city_zip(entry=city_input, opt=input_val):
        city_input_url = "q=" + str(city_input.replace(" ", "%20").lower()) #spaces are replaced with %20
        _access_url(decoded_input=city_input_url)
    else:
        get_user_input_criteria()


def _validate_user_input(raw_input):
    '''
    if-else check for all keystroke entry scenarios
    :param raw_input: user input criteria choice
    :return: none
    '''
    input_check_val = raw_input.lower()
    if input_check_val == 'c':
        _get_city_weather(input_val=input_check_val)
    elif input_check_val == 'z':
        _get_zipcode_weather(input_val=input_check_val)
    elif input_check_val == 'x':
        print("thank you ! have a good day... ")
    else:
        print("invalid entry !!! try again... \n")
        get_user_input_criteria()


def get_user_input_criteria():
    '''
    prompts to choose City, Zip or Quit
    :return: none
    '''
    user_input = input("Choose location criteria to know weather [ C for City, Z for Zip, X to QUIT ] : ")
    _validate_user_input(raw_input=user_input)


def main():
    print(display_app_header.__doc__)
    print("Hello, Welcome to USA_Weather_App !!\n")
    print("Timestamp : " + str(execution_timestamp) + "\n")
    print("Access weather statistics for any Location in USA... \n")
    get_user_input_criteria()


if __name__ == '__main__':                              # runs main() if file wasn't imported
    main()