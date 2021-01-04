import datetime
import json
import os
import time

import mysql.connector
from confluent_kafka import Producer


class AjouParser:
    """
    Ajou notices Parser using Slack API and Apache Kafka (MySQL)
    
    Methods
    -------
    run(server=os.environ["CLOUDKARAFKA_BROKER"], database="notices")
    
    Usage
    -----
        ajou = AjouParser(Kafka_server_ip, mysql_db_name)
        ajou.run()
    """

    # MySQL commands
    UPDATE_LAST_TIME_COMMAND = "UPDATE ajou_notices SET date = %(date)s WHERE id = 1"
    UPDATE_LAST_ID_COMMAND = "UPDATE ajou_notices SET title = %(id)s WHERE id = 1"
    READ_COMMAND = "SELECT * FROM ajou_notices WHERE id > %(id)s ORDER BY id"

    __slots__ = ("db", "cursor", "settings", "producer", "topic")

    def __init__(self, server=os.environ["CLOUDKARAFKA_BROKERS"], database="notices"):
        print("Initializing...")

        # MySQL
        # noticesdb.cr6b18oxlldi.ap-northeast-2.rds.amazonaws.com:3306
        self.db = mysql.connector.connect(
            host=os.environ["MYSQL_ADDRESS"],
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=database,
            charset="utf8",
        )
        self.cursor = self.db.cursor(buffered=True)

        # Kafka
        self.topic = os.environ["CLOUDKARAFKA_TOPIC"]

        self.settings = {  # Producer settings
            "bootstrap.servers": server,
            "compression.type": "snappy",  # High throughput
            # From CloudKarafka
            "session.timeout.ms": 6000,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": os.environ["CLOUDKARAFKA_USERNAME"],
            "sasl.password": os.environ["CLOUDKARAFKA_PASSWORD"],
        }
        self.producer = Producer(self.settings)

    def run(self, period=1805):  # period (second)
        """Check notices from html per period and sends data to Kafka Consumer."""
        p = self.producer
        db = self.db
        cursor = self.cursor

        try:
            while True:  # 30분마다 무한 반복
                print()  # Section
                PRODUCED = 0  # How many messages did it send?

                cursor.execute("SELECT date FROM ajou_notices WHERE id = 1")
                LAST_PARSED = datetime.datetime.strptime(
                    cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S.%f"
                )  # db load date where id = 1

                now = self.getTimeNow()
                diff = (now - LAST_PARSED).seconds

                print("Last parsed at", LAST_PARSED)
                if (diff / period) < 1:  # 업데이트 후 period시간이 안 지났음, 대기
                    print(f"Wait for {period - diff} seconds to sync new posts.")
                    time.sleep(period - diff)

                print("Trying to parse new post(s)...")
                # update last parsed time
                cursor.execute(
                    self.UPDATE_LAST_TIME_COMMAND,
                    {"date": now.strftime("%Y-%m-%d %H:%M:%S.%f")},
                )

                # load last notice id
                cursor.execute("SELECT title FROM ajou_notices WHERE id = 1")
                LAST_PASRED_ID = int(cursor.fetchone()[0])

                cursor.execute(self.READ_COMMAND, {"id": LAST_PASRED_ID})

                noticeId = 0
                for notice in cursor:
                    noticeId = notice[0]
                    kafkaData = self.makeData(
                        noticeId, notice[1], notice[2], notice[3], notice[4]
                    )
                    print("\n>>> Sending a new post...:", noticeId)

                    try:
                        p.produce(
                            self.topic,
                            value=json.dumps(kafkaData[noticeId]),
                            callback=self.acked,
                        )
                    except BufferError as e:
                        print(
                            f"Local producer queue is full. ({len(p)} messages awaiting delivery)"
                        )
                    PRODUCED += 1
                    p.poll(0)  # 데이터 Kafka에게 전송, second

                if PRODUCED:
                    print(f"Sent {PRODUCED} post(s)...")
                    cursor.execute(  # will be saved to "title"
                        self.UPDATE_LAST_ID_COMMAND, {"id": str(noticeId)},
                    )
                    p.flush()
                else:
                    print("\t** No new posts yet")
                print("Parsed at", now)

                db.commit()  # save data
                print(f"Resting {period // 60} minutes...")
                time.sleep(period)

        except Exception as e:  # General exceptions
            print(e)
            print(dir(e))
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
        finally:
            print("\nExiting...")
            cursor.close()
            db.commit()
            db.close()
            p.flush(100)

    # Producer callback function
    @staticmethod
    def acked(err, msg):
        if err is not None:
            print(
                "\t** Failed to deliver message: {0}: {1}".format(
                    msg.value(), err.str()
                )
            )
        else:
            print("Message produced correctly...")

    @staticmethod
    def makeData(postId, postTitle, postDate, postLink, postWriter):
        return {
            postId: {
                "TITLE": postTitle,
                "DATE": postDate,
                "LINK": postLink,
                "WRITER": postWriter,
            }
        }

    @staticmethod
    def getTimeNow() -> datetime.datetime:
        return datetime.datetime.now()


if __name__ == "__main__":
    ajou = AjouParser()
    ajou.run()
