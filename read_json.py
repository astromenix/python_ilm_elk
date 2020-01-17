import json
import requests
import os
from datetime import date, timedelta, datetime

import os
#define variables
shards_name = "pgindex-"

#Collect stats before perform any changes

how_many_days_back_to_delete=7
how_many_days_back_to_readonly=2
d = datetime.now() + timedelta(days=-how_many_days_back_to_delete)
d_how_many_days_back_to_readonly  = datetime.now() + timedelta(days=-how_many_days_back_to_readonly)

def get_total_available_in_bytes():
	dd = datetime.today().strftime('%Y-%m-%d-%H%M%S')
	filename = dd+".data_file.json"
	myCmd = "curl --noproxy localhost, -XGET 'http://localhost:9200/_nodes/stats' > "
	myCmd += filename
	os.system(myCmd)

	with open(filename, "r") as read_file:
    		data = json.load(read_file)

	total_available_in_bytes = 0
	for data_item in data['nodes']:
    		available_in_bytes = int(data["nodes"][data_item]["fs"]["total"]["available_in_bytes"])
    		total_available_in_bytes += available_in_bytes
	return total_available_in_bytes

initial_bytes = 0
initial_bytes = get_total_available_in_bytes()

file2 = open(datetime.today().strftime('%Y-%m-%d') + ".log","a")

file2.write("initial_bytes : " + str(initial_bytes) + "\n")

myCmd = "curl --noproxy localhost, -XGET -H 'Content-Type: application/json' http://localhost:9200/_cat/shards | grep " + shards_name +" | cut -d' ' -f1 | sort -u"
data = os.popen(myCmd).read()

file2.write(data +  "\n")

file2.write("----------------------------------\n")

y = data.split('\n')

file2.write(str(d) + "\n")
file2.write(str(d_how_many_days_back_to_readonly) + "\n")

for string in y:
	if len(string) > 0:  
		int1 = string.find('-')
		int1 += 1
		int2 = len(string)
		str1 = string[int1:int2]
		datetime_object = datetime.strptime(str1, '%Y-%m-%d')
		file2.write( str(datetime_object) + "\n")
		cmd = None
		if d > datetime_object :
			file2.write( str(d) + " is more than " + str(datetime_object)  + "\n")
			cmd = "curl -X PUT -H \"Content-Type: application/json\" -d '{ \"index.blocks.read_only\": \"false\" }' 'http://localhost:9200/" + string  + "/_settings'"
			stream = os.popen(cmd)
                        output = stream.read()
			file2.write( cmd)
			cmd = "curl -XDELETE 'http://localhost:9200/" + string + "/'"
		elif d_how_many_days_back_to_readonly > datetime_object :
			file2.write( str(d_how_many_days_back_to_readonly) +" is more than " + str( datetime_object) + "\n")
			cmd = "curl -X PUT -H \"Content-Type: application/json\" -d '{ \"index\" : { \"number_of_replicas\" : 0, \"blocks.read_only\" : true, \"blocks.read_only_allow_delete\": true  }}' http://localhost:9200/" + string + "/_settings"	
		if cmd is not None:
			stream = os.popen(cmd)
			output = stream.read()
			file2.write( cmd  + "\n")
			file2.write( output  + "\n")
iinal_bytes = 0
final_bytes = get_total_available_in_bytes()

file2.write( "final_bytes:" + str(final_bytes) + "\n")



file2.close() 
