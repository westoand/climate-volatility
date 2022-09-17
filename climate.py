from __future__ import print_function
from pyspark import SparkContext
import timeit

AIR_TEMP_START = 87
AIR_TEMP_END = 92
AIR_TEMP_QUALITY_CODE = 92

AIR_PRESSURE_START = 99
AIR_PRESSURE_END = 104
AIR_PRESSURE_QUALITY_CODE = 104

WIND_SPEED_START = 65
WIND_SPEED_END = 69
WIND_SPEED_QUALITY_CODE = 69

"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_temp_min' and the corresponding
 minimum air temperature value encoded in the input line.
  
 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_temp_min' to temperature
"""


def air_temp_min_mapper(line):
    return ('air_temp_min', int(line[AIR_TEMP_START:AIR_TEMP_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_temp_max' and the corresponding
 maximum air temperature value encoded in the input line.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_temp_max' to temperature
"""


def air_temp_max_mapper(line):
    return ('air_temp_max', int(line[AIR_TEMP_START:AIR_TEMP_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_temp_avg' and the corresponding
 tuple of the air temperature value encoded in the input line and
 unity representing a single sample.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_temp_avg' to tuple temperature, 1
"""


def air_temp_avg_mapper(line):
    return 'air_temp_avg', (int(line[AIR_TEMP_START:AIR_TEMP_END]), 1)


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_pressure_min' and the corresponding
 minimum air pressure value encoded in the input line.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_pressure_min' to pressure
"""


def air_pressure_min_mapper(line):
    return ('air_pressure_min',
            int(line[AIR_PRESSURE_START:AIR_PRESSURE_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_pressure_max' and the corresponding
 maximum air pressure value encoded in the input line.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_pressure_max' to pressure
"""


def air_pressure_max_mapper(line):
    return ('air_pressure_max',
            int(line[AIR_PRESSURE_START:AIR_PRESSURE_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'air_pressure_avg' and the corresponding
 tuple of the air pressure value encoded in the input line and
 unity representing a single sample.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'air_pressure_avg' to tuple temperature, 1
"""


def air_pressure_avg_mapper(line):
    return 'air_pressure_avg', \
           (int(line[AIR_PRESSURE_START:AIR_PRESSURE_END]), 1)


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'wind_speed_min' and the corresponding
 minimum wind speed value encoded in the input line.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'wind_speed_min' to wind speed
"""


def wind_speed_min_mapper(line):
    return ('wind_speed_min',
            int(line[WIND_SPEED_START:WIND_SPEED_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'wind_speed_max' and the corresponding
 maximum wind speed value encoded in the input line.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'wind_speed_max' to wind speed
"""


def wind_speed_max_mapper(line):
    return ('wind_speed_max',
            int(line[WIND_SPEED_START:WIND_SPEED_END]))


"""
 This mapper function maps the parameter input line of NOAA climate 
 data to the key-value pair of 'wind_speed_avg' and the corresponding
 tuple of the wind speed value encoded in the input line and unity 
 representing a single sample.

 @param line is the NOAA climate data sample.
 @return key-value pair of 'wind_speed_avg' to wind speed
"""


def wind_speed_avg_mapper(line):
    return 'wind_speed_avg', \
           (int(line[WIND_SPEED_START:WIND_SPEED_END]), 1)


"""
 This reducer function reduces to the lesser of its two parameter 
 values.

 @param x is a comparable value.
 @param y is a comparable value.
 @return is the lesser value.
"""


def min_reducer(x, y):
    return x if (x < y) else y


"""
 This reducer function reduces to the larger of its two parameter 
 values.

 @param x is a comparable value.
 @param y is a comparable value.
 @return is the larger value.
"""


def max_reducer(x, y):
    return x if (x > y) else y


"""
 This reducer function reduces two tuples to the sum
 of their corresponding components.

 @param x is a tuple to be combined.
 @param y is a tuple to be combined.
 @return is the sum tuple.
"""


def avg_reducer(x, y):
    # combine corresponding tuple components
    return (x[0] + y[0], x[1] + y[1])


"""
 This helper function computes aggregate air temperature statistics
 (min, max, and average) for the parameter RDD.  It also filters
 suspect and erroneous samples.

 @param lines is the RDD of weather samples.
"""


def compute_air_temp_stats(lines):
    clean = lines.filter(
        lambda x: x[AIR_TEMP_QUALITY_CODE] != '2' and
                  x[AIR_TEMP_QUALITY_CODE] != '3' and
                  x[AIR_TEMP_QUALITY_CODE] != '6' and
                  x[AIR_TEMP_QUALITY_CODE] != '7' and
                  (x[AIR_TEMP_QUALITY_CODE] != '9' or
                   x[AIR_TEMP_START:AIR_TEMP_END] != '+9999'))
    min_output = clean.map(air_temp_min_mapper) \
        .reduceByKey(min_reducer)
    print(min_output.collect())
    max_output = clean.map(air_temp_max_mapper) \
        .reduceByKey(max_reducer)
    print(max_output.collect())
    avg_output = clean.map(air_temp_avg_mapper) \
        .reduceByKey(avg_reducer)
    print(avg_output.collect())


"""
 This helper function computes aggregate wind speed statistics
 (min, max, and average) for the parameter RDD.  It also filters
 suspect and erroneous samples.

 @param lines is the RDD of weather samples.
"""


def compute_wind_speed_stats(lines):
    # don't prohibit code 9 for "calm winds" case
    # simply exclude missing values
    clean = lines.filter(lambda x:
                         x[WIND_SPEED_QUALITY_CODE] != '2' and
                         x[WIND_SPEED_QUALITY_CODE] != '3' and
                         x[WIND_SPEED_QUALITY_CODE] != '6' and
                         x[WIND_SPEED_QUALITY_CODE] != '7' and
                         x[WIND_SPEED_START:WIND_SPEED_END] != '9999')
    min_output = clean.map(wind_speed_min_mapper) \
        .reduceByKey(min_reducer)
    print(min_output.collect())
    max_output = clean.map(wind_speed_max_mapper) \
        .reduceByKey(max_reducer)
    print(max_output.collect())
    avg_output = clean.map(wind_speed_avg_mapper) \
        .reduceByKey(avg_reducer)
    print(avg_output.collect())


"""
 This helper function computes aggregate air pressure statistics
 (min, max, and average) for the parameter RDD.  It also filters
 suspect and erroneous samples.

 @param lines is the RDD of weather samples.
"""


def compute_air_pressure_stats(lines):
    clean = lines.filter(
        lambda x: x[AIR_PRESSURE_QUALITY_CODE] != '2' and
                  x[AIR_PRESSURE_QUALITY_CODE] != '3' and
                  x[AIR_PRESSURE_QUALITY_CODE] != '6' and
                  x[AIR_PRESSURE_QUALITY_CODE] != '7' and
                  x[AIR_PRESSURE_START:AIR_PRESSURE_END] != '99999')
    min_output = clean.map(air_pressure_min_mapper) \
        .reduceByKey(min_reducer)
    print(min_output.collect())
    max_output = clean.map(air_pressure_max_mapper) \
        .reduceByKey(max_reducer)
    print(max_output.collect())
    avg_output = clean.map(air_pressure_avg_mapper) \
        .reduceByKey(avg_reducer)
    print(avg_output.collect())


"""
 This driver function computes aggregate weather statistics (min, max,
 and avg) for various weather data fields (air temperature, air
 pressure, and wind speed) using an RDD of NOAA weather data.  It also
 demonstrates filtering the RDD to an arbitrary date.
"""
if __name__ == "__main__":
    sc = SparkContext(appName="PySparkClimate")
    YEAR_START = 1980
    YEAR_END = 2012
    start_time = timeit.default_timer()
    for directory in range(YEAR_START, YEAR_END + 1):
        lines = sc.textFile('/home/DATA/NOAA_weather/' + str(directory), 1)
        print(directory)
        print("Summary Statistics")
        compute_air_temp_stats(lines)
        compute_air_pressure_stats(lines)
        compute_wind_speed_stats(lines)
        print('B-Day')
        # son's due date is April 30th
        b_day = lines.filter(lambda x: x[19:23] == '0430')
        compute_air_temp_stats(b_day)
        compute_air_pressure_stats(b_day)
        compute_wind_speed_stats(b_day)
    print('Time: ', timeit.default_timer() - start_time)
    sc.stop()
