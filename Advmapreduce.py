__author__ = 'Adityatvv'
#To change this template use Tools | Templates.
from pymongo import Connection
from bson.code import Code
# The Map function is executing here
mapper = Code("""
               function () {
                 this.tags.forEach(function(z) {
                   emit(z, 1);
                 });
               }
               """)
# The Reduce function is executing here
reducer = Code("""
                function (key, values) {
                  var total = 0;
                  for (var i = 0; i < values.length; i++) {
                    total += values[i];
                  }
                  return total;
                }
                """)
# Connection established here
connection =  Connection()
db = connection.test
# Fetching for the result with query
result = db.things.map_reduce(mapper, reducer, "myresults",query={"x": {"$lt": 8}})
# Display the result
for result2 in result.find():
 print result2
