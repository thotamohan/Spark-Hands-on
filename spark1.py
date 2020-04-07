import json
import sys
import pyspark
from operator import add
from pyspark import SparkContext

def task1A(rdd):
    count_of_reviews=rdd.count()
    return count_of_reviews

def task1B(rdd,y):
    reviews_year=rdd.map(lambda x:(x['date'],1)).filter(lambda x: int(x[0][0:4])==int(y)).count()
    return reviews_year

def task1C(rdd):
    distinct_users=rdd.map(lambda x:(x['user_id'],1)).distinct().count()
    return distinct_users

def task1D(rdd,m):
    M_users=rdd.map(lambda x:(x['user_id'],1)).reduceByKey(add).sortBy(lambda x:(x[1],x[0]),ascending=False).take(int(m))
    return M_users

def task1E(rdd,n):
    words=rdd.map(lambda x:(x['text'].lower())).flatMap(lambda x:x.split()).map(lambda b : (b,1)).filter(lambda x:x[0] not in stop_words).reduceByKey(add).sortBy(lambda x:(-x[1],x[0]),ascending=True).map(lambda x:x[0]).take(int(n))
    return words

if __name__ == "__main__":
    if len(sys.argv)!=7:
        print("This function needs 6 input arguments <input_file> <output_file> <stopwords>  <y> <m> <n>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    stopwords=sys.argv[3]
    y=sys.argv[4]
    m=sys.argv[5]
    n=sys.argv[6]
    

    sc = SparkContext("local[*]")
    reviews = sc.textFile(input_file).persist()
    rdd=reviews.map(lambda x:json.loads(x))

    
    lineList = [line.rstrip('\n') for line in open(stopwords)]
    punc_words=['[',']','(',')','.',':',';','!','?',',']
    stop_words=lineList+punc_words
    
    out = {}
    t1 = task1A(rdd)
    t2 = task1B(rdd,y)
    t3 = task1C(rdd)
    t4 = task1D(rdd,m)
    t5 = task1E(rdd,n)
    

    out['A'] = t1
    out['B'] = t2
    out['C'] = t3
    out['D'] = t4
    out['E'] = t5
    

    json_out = json.dumps(out)
    with open(output_file,"w") as f:
        f.write(json_out)