import traceback
from consistenthashing.db import Db
from consistenthashing.hash import ConsistentHash

servers = [
        Db(name = "A"),
        Db(name = "B"),
        Db(name = "C"),
        Db(name = "D")
    ]

def hash_func(key: str) -> int:
    total_sum = sum(bytearray(key.encode('utf-8')))
    return total_sum % len(servers)

def save(key: str):
    idx = hash_func(key)
    server = servers[idx]
    print(f"Saved in db: {server.name}")

def get(key: str):
    idx = hash_func(key)
    server = servers[idx]
    print(f"Data received from db: ${server.name}")

def start():
    try:
        consistent_hash = ConsistentHash(servers, 2 ** 10)
        data = ["shubham", "dogra"]
        for key in data:
            idx = consistent_hash.get_node(key)
    except Exception as e:
        print(e, end="\n")
        print(traceback.format_exc(), end="\n")


if __name__ == "__main__":
    start()