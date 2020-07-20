from datetime import datetime
from faker import Faker
from itertools import cycle
from kafkaesk import Application
from models import models
from pydantic import BaseModel
from typing import Optional

import argparse
import asyncio
import random

app = Application(kafka_servers=["localhost:9092"])
fake = Faker()

fake_strings = [fake.text, fake.url, fake.sentence]


def generate_message(model_str: Optional[str]) -> BaseModel:
    if not model_str:
        model = random.choice(list(models.values()))
    else:
        model = models[model_str]

    params = {}
    fake_string_cycle = cycle(fake_strings)
    required = model.schema()["required"]
    for name, props in model.schema()["properties"].items():
        if name in required or random.choice([True, False]):
            if props["type"] == "string":
                params[name] = next(fake_string_cycle)()
            if props["type"] == "integer":
                params[name] = random.randint(0, 999)
            if props["type"] == "date-time":
                params[name] = datetime.now()

    return model(**params)


def register_schemas(only: BaseModel = None) -> None:
    for label, model in models.items():
        app.schema(label)(model)


def list_models() -> None:
    for model in models.values():
        schema = model.schema()
        print(schema["title"])
        print("-" * len(schema["title"]))
        requireds = schema["required"]

        for name, props in schema["properties"].items():
            field_type = props["type"]
            default_value = props.get("default")

            star_label = "*" if name in requireds else ""
            default_label = f"({default_value})" if default_value is not None else ""

            print(f"{name}{star_label}: {field_type} {default_label}")

        print()


async def generate_messages(num_messages: int = 0, model: str = None) -> None:
    register_schemas()
    async with app:
        for idx in range(num_messages):
            msg = generate_message(model_str=model)
            await app.publish("content", msg)


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
