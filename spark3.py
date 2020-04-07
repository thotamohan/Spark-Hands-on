import json
import sys
import pyspark
from operator import add
from pyspark import SparkContext
    
if __name__ == "__main__":
    if len(sys.argv)!=6:
        print("This function needs 6 input arguments  <review_file> <business_file > <output_file> <if_spark> <n>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    partition=sys.argv[3]
    n_reviews=int(sys.argv[5])
    n_partitions=int(sys.argv[4])
    

    default_dict={}
    sc = SparkContext("local[*]") 
    reviews = sc.textFile(input_file)
    if partition=='default':    
        
        review_business=reviews.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],1)).reduceByKey(add).filter(lambda x:x[1]>n_reviews)
        
        no_partitions=review_business.getNumPartitions()
        
        default_dict['n_partitions']=no_partitions
        no_items=review_business.glom().map(len).collect()
        default_dict['n_items']=no_items
        result=review_business.collect()
        default_dict['result']=result

    else:
        n_partition=int(n_partitions)
        customised_review=reviews.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],1)).partitionBy(n_partition,lambda x:hash(x)%n_partition).reduceByKey(add).filter(lambda x:x[1]>int(n_reviews))
        customised_result=customised_review.collect()
        num_partitions=customised_review.getNumPartitions()                
        num_items=customised_review.glom().map(len).collect()
        default_dict['n_partitions']=num_partitions
        default_dict['n_items']=num_items
        default_dict['result']=customised_result
        
    
    with open(output_file,"w") as f:
        json.dump(default_dict,f)   