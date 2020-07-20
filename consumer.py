from kafkaesk import Application
from models import SimpleMessage
from models import SimpleTweet

import asyncio

app = Application(kafka_servers=["localhost:9092"])


@app.subscribe("content", group="example_content_group")
async def messages(data: SimpleMessage) -> None:
    print("SimpleMesage")
    print("------------")
    print(f"Message: {data.message}")
    print(f"Meta: {data.meta}")
    print("- - -")


@app.subscribe("content", group="example_content_group")
async def tweets(data: SimpleTweet) -> None:
    print("SimpleTweet")
    print("-----------")
    print(f"Message: {data.message}")
    print(f"Likes & Retweets: {data.likes} - {data.retweets}")
    print("- - -")


async def main() -> None:
    app.schema("SimpleMessage")(SimpleMessage)
    async with app:
        await app.consume_forever()


if __name__ == "__main__":
    asyncio.run(main())
