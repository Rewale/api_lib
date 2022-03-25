def serialize_message(message: dict) -> str:
    """ Серилизация сообщения в json"""
    return json.dumps(message, ensure_ascii=True, default=str)


def create_hash(message: dict):
    """ Хеш-id """
    return hashlib.md5(serialize_message(message).encode('utf-8')).hexdigest()

