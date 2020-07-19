from kafkaesk import Application
from pydantic import BaseModel

import asyncio
import argparse
from typing import Optional, Union
from models import SimpleMessage

app = Application(kafka_servers=["localhost:9092"])


def list_models() -> None:
    models = [SimpleMessage]
    for model in models:
        schema = model.schema()
        print(schema["title"])
        print("-" * len(schema["title"]))
        for name, props in schema["properties"].items():
            field_type = props["type"]
            print(f"{name}: {field_type}")


async def generate_messages(num_messages: int = 0, model: Optional[Union[BaseModel, str]] = None) -> None:
    app.schema("SimpleMessage")(SimpleMessage)
    async with app:
        for idx in range(num_messages):
            await app.publish("content", SimpleMessage(message="yo", meta=str(idx)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafkaesk publisher playground")
    parser.add_argument("--list-models", action="store_true", help="List models")
    parser.add_argument("--num", type=int, default=10, help="Number of messages")
    parser.add_argument("--model", type=str, help="Model to publish")
    args = parser.parse_args()

    num_messages = args.num
    model = args.model or None

    if args.list_models:
        list_models()
    else:
        asyncio.run(generate_messages(num_messages=num_messages, model=model))
