Weather Observation Tool
------------------------

Python 2.7 is used here.

weather_log_gen.py contains the tool for generating the inputs

usage:

```
python weather_log_gen.py <output file> <total number of data> <probability that the data is invalid>
```

sample usage:
```
python weather_log_gen.py test_inputs.txt 50000 0.33
```


weather_stats.py produces statistics of the flight

usage:

```
# temp units should be kelvin, celsius, fahrenheit
# dist units should be km, miles, or m
python weather_stats.py <input file> <output file> <temp units> <dist units>
```

sample usage:
```
python weather_stats.py input.txt output.txt celsius miles
```
