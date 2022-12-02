import pandas as pd
import csv 
import os

print(os.getcwd())
data = pd.read_csv('./cta_stations.csv')

print(data.columns.tolist())

# data.direction_id = data.direction_id.astype(str)
# data.stop_name =  data.stop_name.astype(str) 
# data.station_name =  data.station_name.astype(str)
# data.station_descriptive_name =  data.station_descriptive_name.astype(str)

data.to_csv("transformed_stations.csv", index=False, header=False, quotechar="'",
      quoting=csv.QUOTE_NONNUMERIC)



#data = pd.read_csv('./transformed_stations.csv')
# data.stop_id = '(' + data.stop_id.astype(str)
# data.direction_id = '"' + data.direction_id.astype(str) + '"'
# data.stop_name =  '"' + data.stop_name.astype(str) + '"'
# data.station_name =  '"' + data.station_name.astype(str) + '"'
# data.station_descriptive_name = '"' + data.station_descriptive_name.astype(str) + '"'
# data['end'] = '),'
# #data.to_csv("transformed_stations.csv", index=False, header=None, quotechar='"', quoting=csv.QUOTE_MINIMAL)

# with open("transformed_stations.txt", 'a') as f:
#      df_as_string = data.to_string(header=False, index=False)
#      f.write(df_as_string)


