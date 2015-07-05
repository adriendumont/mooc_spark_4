```
>>> def get_ratings_tuple(entry):
...     items = entry.split(';')
...     return int(items[0]), int(items[1]), float(items[2])
... 
>>> 
>>> def get_movie_tuple(entry):
...     items = entry.split(';')
...     return int(items[0]), items[1]
... 
>>> 
>>> ratingsRDD = rawRatings.map(get_ratings_tuple).cache()
>>> moviesRDD = rawMovies.map(get_movie_tuple).cache()
>>> 
>>> 
>>> ratingsRDD.take(2)
[(1, 661, 3.0), (1, 1197, 3.0)]
>>> 
>>> moviesRDD.take(2)
[(1, u'Toy Story (1995)'), (2, u'Jumanji (1995)')]
>>> 

```
