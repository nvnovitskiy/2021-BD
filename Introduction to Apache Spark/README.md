# L1 - Introduction to Apache Spark

____

## Задание на лабораторную работу

+ Найти велосипед с максимальным пробегом
```python
bike_max_mileage = trips_.keyBy(lambda x: x.bike_id)

bike_duration = bike_max_mileage.mapValues(lambda x: x.duration).reduceByKey(lambda x1, x2: x1 + x2)

bike_duration_top = bike_duration.top(1, key=lambda x: x[1])[0][0]

bike_duration_top

535
```
+ Найти наибольшее расстояние между станциями
```python
from math import *


def distance_between_stations(latitude1, longitude1, latitude2, longitude2):
  earth_radius = 6371.0088
  latitude1, longitude1 = radians(latitude1), radians(longitude1)
  latitude2, longitude2 = radians(latitude2), radians(longitude2)

  difference_latitude = latitude2 - latitude1
  difference_longitude = longitude2 - longitude1
  
  haversinus_latitutde = (sin(difference_latitude / 2)) ** 2
  haversinus_longitude = (sin(difference_longitude / 2)) ** 2
  
  return 2 * earth_radius * sqrt(haversinus_latitutde + cos(latitude1)\
                                 * cos(latitude2) * haversinus_longitude)


distance = stations_.cartesian(stations_)\
                         .filter(lambda x: x[0].station_id != x[1].station_id)\
                         .map(lambda x: [x[0], x[1], distance_between_stations(x[0].lat, x[0].long, x[1].lat, x[1].long)])\
                         .keyBy(lambda x: (x[0].name, x[1].name))\
                         .reduce(lambda x1, x2: x1 if x1[1] > x2[1] else x2)
                         
                     
print(f"Максимальное расстояние между станциями: {distance[0]}, {distance[1][2]}.")
Максимальное расстояние между станциями: ('Ryland Park', 'Mezes Park'), 34.317914350160784.
```
+ Найти путь велосипеда с максимальным пробегом через станции
```python
bike_path = trips_.filter(lambda x: x.bike_id == bike_duration_top)\
.sortBy(lambda x: x.start_date).map(lambda x: (x.start_station_name, x.end_station_name))

bike_path.first()

('Post at Kearney', 'San Francisco Caltrain (Townsend at 4th)')
```
+ Найти количество велосипедов в системе.
```python
count_bikes = trips_.map(lambda x: x.bike_id).distinct().count()

count_bikes

700
```
+ Найти пользователей потративших на поездки более 3 часов.
```python
users = trips_.filter(lambda x: x.duration > (3 * 60 * 60))\
.map(lambda x: x.zip_code).filter(lambda x: x != "").distinct()

users.take(10)

['58553',
 '94301',
 '94039',
 '94133',
 '93726',
 '94123',
 '4517',
 '29200',
 '45322',
 '94080']
```
