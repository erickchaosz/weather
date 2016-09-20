import sys
import tempfile
import heapq
from datetime import datetime
from collections import defaultdict
from math import hypot

CHUNK_SIZE = 100000

def compose2(f, g):
    return lambda x: f(g(x))

def temp_conversion_table():
    '''
    Uses kelvin and km as standard unit
    '''

    def same_temp(temp):
        return temp

    def kelvin_to_celsius(temp):
        return temp - 273.15

    def kelvin_to_fahrenheit(temp):
        return (temp * 9)/ 5 - 459.67

    def celsius_to_kelvin(temp):
        return temp + 273.15

    def fahrenheit_to_kelvin(temp):
        return (temp + 459.67) * 5 / 9

    table = {
    'kelvin' : {
            'kelvin': same_temp,
            'celsius': kelvin_to_celsius,
            'fahrenheit': kelvin_to_fahrenheit,
        },
    'celsius' : {
            'kelvin': celsius_to_kelvin,
            'celsius': same_temp,
            'fahrenheit': compose2(kelvin_to_fahrenheit, celsius_to_kelvin),
        },
    'fahrenheit': {
            'kelvin': fahrenheit_to_kelvin,
            'celsius': compose2(kelvin_to_celsius, fahrenheit_to_kelvin),
            'fahrenheit': same_temp,
        }
    }
    return table


def distance_conversion_table():
    '''
    Uses km as the standard unit
    '''

    def same_dist(dist):
        return dist

    def km_to_miles(dist):
        return dist * 0.62137

    def miles_to_km(dist):
        return dist * 1.609344

    def km_to_m(dist):
        return dist * 1000

    def m_to_km(dist):
        return dist/1000

    table = {
    'km' : {
            'km': same_dist,
            'm': km_to_m,
            'miles': km_to_miles,
        },
    'm' : {
            'km': m_to_km,
            'm': same_dist,
            'miles': compose2(km_to_miles, m_to_km),
        },
    'miles': {
            'km': miles_to_km,
            'm': compose2(km_to_m, miles_to_km),
            'miles': same_dist,
        }
    }
    return table


TEMP_CONVERSION = temp_conversion_table()
DIST_CONVERSION = distance_conversion_table()

def parse(text):
    '''
    Returns None if text is invalid else returns a tuple of
    (timestamp, x, y, temperature, observatory)
    '''
    try:
        text = text.strip()
        data = text.split('|')

        timestamp = datetime.strptime(data[0], "%Y-%m-%dT%H:%M")
        location = data[1].split(',')
        x = int(location[0])
        y = int(location[1])
        temperature = float(data[2])
        observatory = data[3]
        return (timestamp, x, y, temperature, observatory)

    except Exception as e:
        return None


def stringify(weather_data):
    weather_format = "{timestamp}|{x}, {y}|{temperature}|{observatory}\n"

    timestamp, x, y, temperature, observatory = weather_data
    timestamp = timestamp.strftime("%Y-%m-%dT%H:%M")

    return weather_format.format(timestamp=timestamp,
                                         x=x,
                                         y=y,
                                         temperature=temperature,
                                         observatory=observatory)

def read_weather_data(f):
    for line in f:
        weather_data = parse(line)
        if weather_data:
            yield weather_data

def sort_chunks(filename):
    '''
    Sorts chunks of the file filename into temporary files
    and return a list of iterables from those files
    '''

    def flush_chunk(weather_list):
        'Writes chunk to memory and return an iterable from it'
        tempf = tempfile.TemporaryFile()
        tempf.write(''.join(map(stringify, weather_list)))
        tempf.seek(0)
        return read_weather_data(tempf)

    # contains the iterables gotten from sorted chunks inside temp file
    iters = []

    # temporary buffer for sorting before written to temp file
    weather_list = []

    with open(filename, 'r') as f:
        for weather_data in read_weather_data(f):
            weather_list.append(weather_data)

            # writes to temp file and gets an iterable from it if chunk is filled up
            if len(weather_list) > CHUNK_SIZE:

                # sort chunk in memory by timestamp
                weather_list.sort(key=lambda x: x[0])

                iters.append(flush_chunk(weather_list))
                weather_list = []

        # Necessary for last chunk
        weather_list.sort(key=lambda x: x[0])
        iters.append(flush_chunk(weather_list))

    return iters


def external_sort(filename):
    '''
    Sorts the file by first splitting it into chunks, sorting the chunks
    and writing them to temp files
    Merge sort is then used to sort those temp files
    Returns an iterable from the sorted file
    '''

    iters = sort_chunks(filename)

    # contains current sorted chunk
    sorted_weather = []
    tempf = tempfile.TemporaryFile()

    # heapq.merge merges sorted iterables and returns an iterable
    for weather_res in heapq.merge(*iters):
        sorted_weather.append(weather_res)
        if len(sorted_weather) > CHUNK_SIZE:
            tempf.write(''.join(map(stringify, sorted_weather)))
            sorted_weather = []

    # writes the last chunk
    tempf.write(''.join(map(stringify, sorted_weather)))
    sorted_weather = []

    tempf.seek(0)

    return read_weather_data(tempf)

def convert_units(weather_data, temp_units, dist_units):
    timestamp, x, y, temperature, observatory = weather_data

    # Do unit conversions
    if observatory == 'AU':
        temperature = TEMP_CONVERSION['celsius'][temp_units](temperature)
        x = DIST_CONVERSION['km'][dist_units](x)
        y = DIST_CONVERSION['km'][dist_units](y)
    elif observatory == 'US':
        temperature = TEMP_CONVERSION['fahrenheit'][temp_units](temperature)
        x = DIST_CONVERSION['miles'][dist_units](x)
        y = DIST_CONVERSION['miles'][dist_units](y)
    elif observatory == 'FR':
        temperature = TEMP_CONVERSION['kelvin'][temp_units](temperature)
        x = DIST_CONVERSION['m'][dist_units](x)
        y = DIST_CONVERSION['m'][dist_units](y)
    else:
        temperature = TEMP_CONVERSION['kelvin'][temp_units](temperature)
        x = DIST_CONVERSION['km'][dist_units](x)
        y = DIST_CONVERSION['km'][dist_units](y)

    return (timestamp, x, y, temperature, observatory)


def weather_stats(filename, temp_units, dist_units):
    min_temp = None
    max_temp = None
    mean_temp = None
    observatory_freq = defaultdict(int)
    total_distance = 0

    prev_location = None
    total_temp = 0
    weather_data_count = 0

    for weather_data in external_sort(filename):
        weather_data_count += 1
        timestamp, x, y, temperature, observatory = convert_units(weather_data,
                                                                 temp_units,
                                                                 dist_units)

        # First data
        if prev_location == None:
            min_temp = temperature
            max_temp = temperature
        else:
            min_temp = min(min_temp, temperature)
            max_temp = max(max_temp, temperature)
            prev_x, prev_y = prev_location
            total_distance += hypot(x - prev_x, y - prev_y)

        prev_location = (x, y)
        total_temp += temperature
        observatory_freq[observatory] += 1

    mean_temp = total_temp / weather_data_count

    return (min_temp, max_temp, mean_temp, observatory_freq, total_distance)

if __name__ == "__main__":
    filename = sys.argv[1]
    output_filename = sys.argv[2]
    temperature_units = sys.argv[3]
    distance_units = sys.argv[4]

    min_temp, max_temp, mean_temp, observatory_freq, total_distance = weather_stats(filename, temperature_units, distance_units)
    stats_format = '''Min temperature: {min_temp}
Max temperature: {max_temp}
Mean temperature: {mean_temp}
Total distance: {total_distance}
'''

    stats_text = stats_format.format(min_temp=min_temp,
                                     max_temp=max_temp,
                                     mean_temp=mean_temp,
                                     total_distance=total_distance)

    observatory_text = 'Observatory stats: \n'
    for (observatory, freq) in observatory_freq.iteritems():
        observatory_text += '\t{observatory}: {freq}\n'.format(observatory=observatory,
                                                             freq=freq)

    with open(output_filename, 'w') as f:
        f.write(stats_text + observatory_text)
