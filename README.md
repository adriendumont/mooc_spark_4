```
/home/student/mooc/movielens-bench/ml-1m/ratings.dat
/home/student/mooc/movielens-bench/ml-1m/README
/home/student/mooc/movielens-bench/ml-1m/users.dat
/home/student/mooc/movielens-bench/ml-1m/ratings.dat.gz
/home/student/mooc/movielens-bench/ml-1m/movies.dat

ratingsFilename = '/home/student/mooc/movielens-bench/ml-1m/ratings.dat'
moviesFilename = '/home/student/mooc/movielens-bench/ml-1m/movies.dat'

def get_ratings_tuple(entry):
    items = entry.split(';')
    return int(items[0]), int(items[1]), float(items[2])

def get_movie_tuple(entry):
    items = entry.split(';')
    return int(items[0]), items[1]



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

def sortFunction(tuple):
    key = unicode('%.3f' % tuple[0])
    value = tuple[1]
    return (key + ' ' + value)


>>> sortFunction( (1, u'Toy Story (1995)') )
u'1.000 Toy Story (1995)'

>>> print oneRDD.sortBy(sortFunction, True).collect()
[(1, u'alpha'), (1, u'delta'), (1, u'epsilon'), (2, u'alpha'), (2, u'beta'), (3, u'alpha')]
>>> print twoRDD.sortBy(sortFunction, True).collect()
[(1, u'alpha'), (1, u'delta'), (1, u'epsilon'), (2, u'alpha'), (2, u'beta'), (3, u'alpha')]


def getCountsAndAverages(IDandRatingsTuple):
    from operator import add
    tuple_size = len(IDandRatingsTuple[1])
    tuple_total = reduce(add, IDandRatingsTuple[1])
    return ( IDandRatingsTuple[0], (tuple_size, tuple_total / float( tuple_size )) )

movieIDsWithRatingsRDD = ratingsRDD.map(lambda (uid,mid,rating): (mid,rating)).groupByKey()

>>> movieIDsWithRatingsRDD.map(getCountsAndAverages).take(1)
[(2048, (131, 3.49618320610687))]

>>> moviesRDD.lookup(2048)[0]
u'Great Mouse Detective, The (1986)'
>>> moviesRDD.take(3)
[(1, u'Toy Story (1995)'), (2, u'Jumanji (1995)'), (3, u'Grumpier Old Men (1995)')]

```
[http://ge.tt/5Fa24tJ2/v/0](http://ge.tt/5Fa24tJ2/v/0)
```
def squaredError(label, prediction):
    aux = label - prediction
    return aux * aux
def calcRMSE(labelsAndPreds):
    sumOfSquares = labelsAndPreds.reduce(lambda tpl1, tpl2: squaredError(tpl1) + squaredError(tpl2))
    return np.sqrt(sumOfSquares / labelsAndPreds.count())
```
[ipynb](https://gist.github.com/dserban/8d78ecfcaa0b061d8c81)

[lab4](https://gist.github.com/dserban/97e507ef8499f4532694)
