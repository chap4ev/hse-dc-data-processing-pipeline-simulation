import time
import random
import hashlib
from datetime import datetime
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adjust the broker address as per your configuration
}

# Create Producer instance
producer = Producer(conf)

# List of possible latitude and longitude (approximate range for Russian Federation)
lat_range = (41.1856, 82.0585)
lon_range = (19.6389, 179.9999)

# Sensor ID generator (8-digit unique code)
def generate_sensor_id():
    return '{:08d}'.format(random.randint(0, 99999999))

# Controller ID generator (hash from sensor id)
def generate_controller_id(sensor_id):
    return hashlib.md5(sensor_id.encode()).hexdigest()

def main():
    while True:
        # Get current UTC event date and time
        event_datetime = datetime.utcnow().isoformat()

        # Generate random sensor ID
        sensor_id = generate_sensor_id()

        # Generate random location within Russia
        latitude = round(random.uniform(*lat_range), 6)
        longitude = round(random.uniform(*lon_range), 6)

        # Generate random temperature between -20 and +20
        temperature = round(random.uniform(-20, 20), 2)

        # Generate Controller ID from Sensor ID
        controller_id = generate_controller_id(sensor_id)

        # Create data payload
        data = {
            "event_datetime": event_datetime,
            "sensor_id": sensor_id,
            "latitude": latitude,
            "longitude": longitude,
            "temperature": temperature,
            "controller_id": controller_id
        }

        # Send data to Kafka topic
        producer.produce('sensor-data', value=str(data))
        
        # Print the produced data
        print(f"Produced: {data}")

        # Wait for one second
        time.sleep(1)

        # Poll to handle events and callbacks
        producer.poll(0)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        # Wait for any outstanding messages to be delivered 
        producer.flush()
