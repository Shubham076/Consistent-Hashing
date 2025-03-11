import hashlib
from bisect import bisect, bisect_left
from consistenthashing.db import Db


class ConsistentHash:
    def __init__(self, nodes: list[Db], total_slots: int):
        if total_slots > 2 ** 256:
            raise Exception("max total tokens limit reached")
        self.__nodes = []
        self.__keys = []
        self.__total_slots = total_slots
        for node in nodes:
            self.add_node(node)

    def hash_func(self, key: str) -> int:
        hash_fn = hashlib.sha256()
        hash_fn.update(bytearray(key.encode("utf-8")))
        sha256_hash = hash_fn.hexdigest()
        return int(sha256_hash, 16) % self.__total_slots

    def add_node(self, node: Db):
        if len(self.__keys) == self.__total_slots:
            raise Exception("hash space is full")

        # find place on the hash ring
        key = self.hash_func(node.name)

        # find the idx where key needs to be inserted
        idx = bisect(self.__keys, key)

        # perform data migration

        self.__nodes.insert(idx, node)
        self.__keys.insert(idx, key)

    def remove_node(self, node: Db):
        if len(self.__keys) == 0:
            raise Exception("hash space is empty")

        # find place on the hash ring
        key = self.hash_func(node.name)
        idx = bisect_left(self.__keys, key)

        if idx >= len(self.__keys) or self.__keys[idx] != key:
            raise Exception("key does not exist")

        # perform data migration
        self.__keys.pop(idx)
        self.__nodes.pop(idx)

    def get_node(self, item: str) -> int:
        key = self.hash_func(item)

        # if key doesn't exist circle back
        idx = bisect(self.__keys, key) % len(self.__keys)
        return idx


