import json
import sys
import pyspark
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    print(len(sys.argv))   
    if len(sys.argv)!=6:
        print("This function needs 6 input arguments  <review_file> <business_file > <output_file> <if_spark> <n>")
        sys.exit(1)
    
    review_file = sys.argv[1]
    business_file = sys.argv[2]
    output_file=sys.argv[3]
    if_spark=sys.argv[4]
    n=sys.argv[5]
    
    
  
    sc = SparkContext("local[*]")
    reviews = sc.textFile(review_file)
    business = sc.textFile(business_file)
    
    

    if if_spark=='spark':
        def split_function(string):
            final_list = []
            list_strings=string.split(',')
            for item in list_strings:
                item=item.strip()
                final_list.append(item)
            return final_list


        rdd = reviews.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],(x['stars'],1))).reduceByKey(lambda acc,n: (acc[0]+n[0],acc[1]+n[1]))



        rdd_business=business.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],x['categories'])).filter(lambda x:x[1] is not None).map(lambda x:(x[0],split_function(x[1])))


        joined_rdd=rdd_business.join(rdd)

        res_final=joined_rdd.flatMap(lambda x : [(y,x[1][1]) for y in x[1][0]]).reduceByKey(lambda acc,n: (acc[0]+n[0],acc[1]+n[1])).map(lambda x:(x[0],x[1][0]/x[1][1])).sortBy(lambda x:(-x[1],x[0])).take(int(n))

        out={}
        out['result']=res_final
        #print(out)

        json_out = json.dumps(out)
        with open(output_file,"w") as f:
            f.write(json_out)



    else:
        line_contents=[]
        with open(business_file) as fin:
            for line in fin:
                line_contents.append(json.loads(line))
        business_dict={}
        def add(key,value):
            business_dict[key]=value  
        for item in line_contents:
            add(item['business_id'],item['categories'])
        updated_dict={k: v for k, v in business_dict.items() if v is not None}



        data = [json.loads(line) for line in open(review_file, 'r')]
        review_dict={}
        for item in data:
            if item['business_id'] in review_dict.keys():
                value=review_dict[item['business_id']]
                review_dict[item['business_id']]=((value[0]+item['stars']),(value[1]+1))  
            else:
                review_dict[item['business_id']]=(item['stars'],1)


        merged_dict={}
        def combine_dict(d1, d2):
            for k in d1.keys():
                if k in d2.keys():
                    lists=[]
                    lists.append(d2[k])
                    lists.append(d1[k])
                    merged_dict[k]=lists
            return merged_dict

        merged_dict=combine_dict(review_dict,updated_dict)

        temp_dict={}
        for values in merged_dict.values():
            for items in values[0].split(','):
                items=items.strip()
                if items in temp_dict.keys():
                    val=temp_dict[items]
                    temp_dict[items]=((val[0]+values[1][0]),(val[1]+values[1][1]))
                else:
                    temp_dict[items]=values[1]


        for keys in temp_dict.keys():
            temp_dict[keys]=temp_dict[keys][0]/temp_dict[keys][1]

        top_categories=sorted(temp_dict.items(), key=lambda x: (-x[1],x[0]), reverse=False)

        out2={}
        out2['result']=top_categories[:int(n)]


        json_out = json.dumps(out2)
        with open(output_file,"w") as f:
            f.write(json_out)