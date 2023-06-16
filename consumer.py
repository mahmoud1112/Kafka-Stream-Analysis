

from kafka import KafkaConsumer
import json
import telebot
import pymongo



if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    mycol = mydb["users"]  
    bot = telebot.TeleBot("6100639297:AAEZKjeFa1iDSj0HXr81-Q6VPvqx5ab15UA")
    consumer = KafkaConsumer(
        "vid_list",
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a")
    for msg in consumer:
        bot.send_message(1221050071, "change in this video details {}".format(json.loads(msg.value)))
        mycol.insert_one(json.loads(msg.value))
