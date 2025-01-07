from dotenv import load_dotenv
import os
import asyncio

# Load environment variables from .env file
load_dotenv(dotenv_path="/home/jovyan/work/streaming_library/.env")

API_URL = os.getenv("API_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
EARTHSCOPE_USERNAME = os.getenv("EARTHSCOPE_USERNAME")
EARTHSCOPE_PASSWORD = os.getenv("EARTHSCOPE_PASSWORD")


earthscope_topic_metadata = {
  "dataset_name": "earthscope_kafka_gnss_observations",
  "dataset_title": "earthscope_kafka_gnss_observations",
  "owner_org": "test_organization",
  "kafka_topic": "public.gnss.positions.normalized.geojson.compact",
  "kafka_host": "b-2-public.kafkaprodpub.2c30l9.c4.kafka.us-east-2.amazonaws.com",
  "kafka_port": "9196", 
  "extras": {
    "sasl_mechanism": "SCRAM-SHA-512",
    "security_protocol": "SASL_SSL"
  }
}

filters = [
    "SNCL IN ['P505.PW.LY_.00', 'DHLG.CI.LY_.20', 'P159.PW.LY_.00']",
    "IF Q > 2000000 THEN alert = blue",
    "IF Q <= 2000000 THEN alert = red",
    "IF alert = 'red' THEN rate = 2"
]