import hashlib
import bisect

class ConsistentHash:
    def __init__(self, nodes=None, replicas=100):
        """Initialize consistent hash.
        
        nodes: List of actual nodes.
        replicas: Number of virtual nodes.
        """
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """Add a node to the hash ring (both actual and virtual nodes)."""
        for i in range(self.replicas):
            virtual_node = f"{node}#{i}"
            key = self.hash_key(virtual_node)
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def remove_node(self, node):
        """Remove node from the hash ring."""
        for i in range(self.replicas):
            virtual_node = f"{node}#{i}"
            key = self.hash_key(virtual_node)
            del self.ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key):
        """Return the node responsible for the given key."""
        if not self.ring:
            return None

        key_hash = self.hash_key(key)
        # Find the server that is closest on the right side of the given key hash.
        index = bisect.bisect_right(self.sorted_keys, key_hash)
        
        # If the key hash is larger than the largest server hash, wrap around to the beginning.
        if index == len(self.sorted_keys):
            index = 0

        return self.ring[self.sorted_keys[index]]

    @staticmethod
    def hash_key(key):
        """Return a hash value of the key."""
        m = hashlib.md5()
        m.update(key.encode("utf-8"))
        return int(m.hexdigest(), 16)


# Example Usage:
hash_ring = ConsistentHash(["node1", "node2", "node3"])
print(hash_ring.get_node("some_key1"))
print(hash_ring.get_node("some_key2"))
