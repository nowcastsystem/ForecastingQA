import os
import pymongo
from bson.json_util import dumps
from QUANTAXIS.TSBoosting.TSBoosting import TS_Boosting_predict


start = '2007-10-18'
end = '2019-08-18'
by = 'D'
client = pymongo.MongoClient("mongodb://10.0.75.1:27017/")
change_stream = client.mydatabase.rawdatatest.watch()
print('start listen')

for change in change_stream:
    TS_Boosting_predict(start=start,end=end,by=by,databaseid='mydatabase',collectionid='rawdatatest')
#     print("change")
#     break
# while True:
#     TS_Boosting_predict(start=start,end=end,by=by,databaseid='mydatabase',collectionid='rawdatatest')
# with client.mydatabase.rawdatatest.watch() as stream:
#     while stream.alive:
#         TS_Boosting_predict(start=start, end=end, by=by, databaseid='mydatabase', collectionid='rawdatatest')
#     change["clusterTime"]


