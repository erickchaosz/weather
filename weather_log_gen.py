import sys
import random
from datetime import datetime
import string

# min and max temperatures
MIN_TEMP = 100
MAX_TEMP = 1000

# min and max locations
MIN_LOC = 1
MAX_LOC = 10000000



def gen_timestamp():
    year = random.randint(1990, 2016)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    timestamp = datetime(year, month, day, hour, minute)
    return timestamp.strftime("%Y-%m-%dT%H:%M")

def gen_location():
    x = random.randint(MIN_LOC, MAX_LOC)
    y = random.randint(MIN_LOC, MAX_LOC)
    return (x, y)

def gen_temperature():
    'Generates floating point number in 2 decimal places'
    return round(random.uniform(MIN_TEMP, MAX_TEMP), 2)

def gen_observatory():
    observatories = ['AU', 'US', 'FR', 'IDN', 'CN']
    return random.choice(observatories)

def format_weather_data(timestamp, x, y, temperature, observatory):
    weather_format = "{timestamp}|{x}, {y}|{temperature}|{observatory}\n"
    return weather_format.format(timestamp=timestamp,
                                         x=x,
                                         y=y,
                                         temperature=temperature,
                                         observatory=observatory)

def gen_valid_inputs():
    timestamp = gen_timestamp()
    x, y = gen_location()
    temperature = gen_temperature()
    observatory = gen_observatory()

    return (timestamp, x, y, temperature, observatory)

def gen_valid_weather_data():
    timestamp, x, y, temperature, observatory = gen_valid_inputs()

    weather_data = format_weather_data(timestamp, x, y, temperature, observatory)
    return weather_data

def corrupt_data(data):
    'Assumes data is string, corrupt it by adding random chars'

    num_chars = random.randint(1,3)
    # list of random characters
    rand_chars = [random.choice(string.ascii_lowercase[6:]) for _ in range(num_chars)]

    # combine random characters with valid data and shuffle them
    w_list = list(data) + rand_chars
    random.shuffle(w_list)

    return ''.join(w_list)

def gen_invalid_format(timestamp, x, y, temperature, observatory):
    'Removes bars from the valid format string to make it invalid'

    valid_format = format_weather_data(timestamp, x, y, temperature, observatory)
    num_chars = random.randint(1, 3)

    return valid_format.replace('|', '', num_chars)


def gen_invalid_inputs():
    'Randomly corrupt some inputs'
    pick = random.random()

    timestamp = gen_timestamp()
    x, y = gen_location()
    temperature = gen_temperature()
    observatory = gen_observatory()

    if pick <= 0.3:
        timestamp = corrupt_data(str(timestamp))
    elif pick > 0.3 and pick < 0.7:
        temperature = corrupt_data(str(temperature))
    else:
        x = corrupt_data(str(x))
        y = corrupt_data(str(y))

    return (timestamp, x, y, temperature, observatory)

def gen_invalid_weather_data():
    '''
    Does one of 3 things:
    1. Gives valid format with invalid inputs
    2. Gives invalid format with valid inputs
    3. Gives invalid format and invalid inputs
    '''

    pick = random.random()

    # valid format invalid inputs
    if pick <= 0.3:
        timestamp, x, y, temperature, observatory = gen_invalid_inputs()
        data = format_weather_data(timestamp, x, y, temperature, observatory)

    # invalid format valid inputs
    elif pick > 0.3 and pick < 0.7:
        timestamp, x, y, temperature, observatory = gen_valid_inputs()
        data = gen_invalid_format(timestamp, x, y, temperature, observatory)

    # invalid format invalid inputs
    else:
        timestamp, x, y, temperature, observatory = gen_invalid_inputs()
        data = gen_invalid_format(timestamp, x, y, temperature, observatory)

    return data


if __name__ == "__main__":
    filename = sys.argv[1]
    num_data = int(sys.argv[2])
    invalid_data_prob = float(sys.argv[3])

    valid_data_count = 0
    invalid_data_count = 0

    with open(filename, 'w') as f:
        for i in range(num_data):
            pick = random.random()

            if pick > invalid_data_prob:
                weather_data = gen_valid_weather_data()
                valid_data_count += 1
            else:
                weather_data = gen_invalid_weather_data()
                invalid_data_count += 1

            f.write(weather_data)

    print "Valid data count: " + str(valid_data_count)
    print "Invalid data count: " + str(invalid_data_count)
