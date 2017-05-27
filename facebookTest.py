#import friends.protocols.facebook as facebook

import facebook,json,findspark

findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext



#from pyspark import SparkConf,SparkContext
#from pyspark.sql import SQLContext,Row

#from pyspark.sql.types import StringType,StructField,StructType

#findspark.init()

graph = facebook.GraphAPI(access_token='1257032691047126|mP2cUAf8cIH-ZA0WntiqScJT8HI', version='2.9')

#pages = graph.search(type = 'page',q='narendramodi',fields='fan_count,talking_about_count')

posts = graph.get_object(id='narendramodi',fields='posts.limit(50)')['posts']['data']
#print graph.get_connections(id='168926019255_10154922404214256', connection_name='likes', summary='true')
postids = []

for key in posts:
    postids.append(key['id'])
    #print key

#print postids

from pyspark.sql.types import StringType,StructField,StructType

schema = StructType([StructField('id',StringType()),
                     StructField('from_name',StringType()),
                     StructField('from_id',StringType()),
                     StructField('message',StringType()),
                     StructField('post_link',StringType()),
                     StructField('likes_count',StringType()),
                     StructField('comments_count',StringType())])

posting = graph.get_objects(postids,fields='from,comments.limit(1).summary(true),likes,message,reactions.type(LIKE).summary(total_count).limit(0).as(like),permalink_url',limit=1,summary='true')

postsData = {}

#print posting
#for key in posting:
#   print posting[key]['id']
#   print posting[key]['from']['name'],posting[key]['from']['id']
#   print posting[key]['message']
#   print posting[key]['permalink_url']
#   print posting[key]['like']['summary']['total_count']

postList = []

for key in posting:
    #comment_count = ''#graph.get_connections(id=posting[key]['id'],connection_name='comments',summary='true')['summary']['total_count']
    comment_count = posting[key]['comments']['summary']['total_count']
    data = ((posting[key]['id']).encode('utf8')+'\t'+(posting[key]['from']['name']).encode('utf8')
     +'\t'+(posting[key]['from']['id']).encode('utf8')+'\t'+(posting[key]['message']).encode('utf8')
     +'\t'+(posting[key]['permalink_url']).encode('utf8')+'\t'+ str(posting[key]['like']['summary']['total_count']).encode('utf8')
     +'\t'+str(comment_count))
    postList.append(data)



 #  postList.append(Row((posting[key]['id']).decode('utf8').strip(),
 #                   (posting[key]['from']['name']).decode('utf8').strip(),
 #                   (posting[key]['from']['id']).decode('utf8').strip(),
 #                   (posting[key]['message']).decode('utf8').strip(),
 #                   (posting[key]['permalink_url']).decode('utf8').strip(),
 #                   str(posting[key]['like']['summary']['total_count']).decode('utf8').strip()))

#for x in postList:
  # print type(x)
conf = SparkConf().setMaster("local[2]").setAppName("FaceBook")
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sqlContext = SQLContext(sparkContext=sc)
#sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

from pyspark.sql import Row

def processRecord(record):
    words = record.split('\t')
    return Row(words[0],words[1],words[2],words[3],words[4],words[5],words[6])


data = sc.parallelize(postList).map(lambda x : processRecord(x))

sqlContext.createDataFrame(data,schema=schema).show(100)

#data = sc.parallelize(postList)

#data.foreach(lambda x : processRecord(x))

#df = sqlContext.createDataFrame(data,schema=schema)

#df.show()


#df = sqlContext.read.json(data)


#df.show()

#for key in posting :
#    print posting[key]['id']
#    comments = graph.get_all_connections(id=posting[key]['id'],connection_name='reactions',summary='true',limit=10)
#    for key in comments :
#       print key
      #  if key == 'total_count' :
      #    print comments[key]#['summary']#['summary']['total_count']
    #commentfields = graph.get_object(id=posting[key]['id'],fields='from,message',summary='true')

    #for key in commentfields :
    #    print commentfields['message']#['from']#['id'],commentfields[key]['from']['name']
    #    print commentfields['from']#['message']


   #for key2 in posting[key] :
    #   print posting[key][key2]

"""reactions.type(LIKE).summary(total_count).limit(0).as(like),reactions.type(LOVE).summary(total_count).limit(0).as(love),reactions.type(WOW).summary(total_count).limit(0).as(wow),reactions.type(HAHA).summary(total_count).limit(0).as(haha),reactions.type(SAD).summary(total_count).limit(0).as(sad),reactions.type(ANGRY).summary(total_count).limit(0).as(angry)'"""

#likes = graph.get_object(id='177526890164_10158733411115165',fields='comments,summery=true',summery='true')



#for key in likes['comments'] :
#    print key