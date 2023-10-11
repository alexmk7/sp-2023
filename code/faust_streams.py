import faust
import random
import re
import configparser


def gen_text() -> str:
    words = list({"hello", "world", "hey", "no", "yes"})
    return " ".join(random.choice(words) for _ in range(random.randint(2, 10)))


class Text(faust.Record):
    text: str


def main(topic: str, bootstrap_servers: str):
    app = faust.App("hello-app", broker=bootstrap_servers)

    text_topic = app.topic(topic, key_type=bytes, value_type=bytes, partitions=1)

    counts = app.Table(
        "word-counts", key_type=bytes, value_type=int, default=int, partitions=1
    ).tumbling(size=10, expires=5, key_index=True)

    @app.agent(text_topic)
    async def text_topic_listener(texts):
        async for text in texts:
            for word in re.findall("\w+", text.decode("utf-8").lower()):
                counts[word] += 1
            print(list(counts.items()))

    @app.timer(interval=4.0)
    async def text_sender(app):
        await text_topic.send(
            value=gen_text(),
        )

    app.main()


if __name__ == "__main__":
    cfg = configparser.ConfigParser()
    cfg.read("properties.ini")

    main(cfg["Kafka"]["topic"], cfg["Kafka"]["bootstrap_servers"])
