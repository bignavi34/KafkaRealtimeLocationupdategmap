from confluent_kafka import Producer
import json
import os
import time
conf={'bootstrap.servers':'localhost:9092'}
producer =Producer(**conf)
start_latitude = 19.07834
start_longitude =72.87777
end_latitude =18.2345
end_longitude =73.8976
num_step=1000
step_size_lat=(end_latitude-start_latitude)/num_step
step_size_long=(end_longitude-start_longitude)/num_step
current_step=0
def delivery_report(err,msg):
    if err is not None:
        print("Delivery report:{err}")
    else:
        print(f"Delivery report{msg.topic()}{msg.partition()}")
        
topic = 'location_updates'
while True:
    latitude=start_latitude+step_size_lat+current_step
    longitude=start_longitude+step_size_long+current_step
    data={'latitude':latitude, 'longitude':longitude}
    print(data)
    producer.produce(topic,json.dumps(data).encode('utf-8'),callback=delivery_report)
    producer.flush()
    current_step+=1
    if current_step>num_step:
        current_step=0
    time.sleep(5)
        
    