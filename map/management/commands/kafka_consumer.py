from django.core.management.base import BaseCommand
from confluent_kafka import Consumer,KafkaException
import json
from map.models import LocationUpdate
import os
import time
class Command(BaseCommand):
    help="run a location update"
    def handle(self, *args,**options):
        conf={'bootstrap.servers':'localhost:9092',
              'group.id':"location_updates",
              'auto.offset.reset':'earliest',
             }

        consumer=Consumer(conf)
        consumer.subscribe(['location_updates'])
        try:
            while True:
                msg=consumer.poll(timeout=5.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() ==KafkaException: 
                        continue
                    else:
                        print(msg.error())
                        break
                data=json.loads(msg.value().decode('utf-8'))
                LocationUpdate.objects.create(
                latitude=data['latitude'],
                longitude=data['longitude']
                )
                print(f"received location{data}")
                time.sleep(2)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
                
