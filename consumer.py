from kafkaesk import Application

import asyncio


from models import SimpleMessage

app = Application(kafka_servers=["localhost:9092"])


@app.subscribe("content", group="example_content_group")
async def messages(data: SimpleMessage):
    print(f"{data.message}\n{data.meta}")


async def main():
    app.schema("SimpleMessage")(SimpleMessage)
    async with app:
        await app.consume_forever()


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Kafkaesk pubsub playground")
    # parser.add_argument("-n", "--url", help="Number of messages")
    # parser.add_argument("-l", "--max-levels", type=int, help="Max nested levels")
    # options = parser.parse_args()
    asyncio.run(main())
    # asyncio.run(generate_data())
