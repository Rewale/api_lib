import json
import asyncio
import aioredis
import aio_pika
from aioredis import Redis


async def make_connection_amqp(address):
    """ Создаем подключение и открываем канала единожды"""
    conn = await aio_pika.connect_robust(
        address, loop=asyncio.get_running_loop()
    )
    ch = await conn.channel()

    return conn, ch


async def listen_rabbit_write_redis(queue_name):
    # Declaring queue
    try:
        queue = await channel.declare_queue(name=queue_name)
    except Exception as e:
        print(e)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            message: aio_pika.IncomingMessage
            with message.process():
                decoded = message.body.decode()
                json_dict = json.loads(decoded)
                try:
                    corr_id = json_dict['response_id']
                except:
                    await message.reject()
                    continue
                del json_dict['response_id']
                value = json.dumps(json_dict)
                await redis.set(corr_id, value)


async def start_listening(queue_name: str, connection_amqp: str, connection_redis: str = "redis://localhost"):
    global redis
    global connection
    global channel
    redis = aioredis.from_url(connection_redis)
    connection, channel = await make_connection_amqp(connection_amqp)
    await listen_rabbit_write_redis(queue_name)
