from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Dstreams

#Contexto

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc,10) 

lines = ssc.socketTextStream('localhost', 9090)

# Creamos un servidor con "nc -lk" 9090 , esto abre un socket para que se conecten otros.

#Procesamiento de datos
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x,y : x + y)

wordCounts.pprint() #imprime los primeros 10 registros 

# Inicializamos el contexto

ssc.start()
ssc.awaitTermination()
