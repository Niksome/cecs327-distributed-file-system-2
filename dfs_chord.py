import os
import json
import hashlib
from bisect import bisect_left
from typing import Dict, List, Optional, Any


def sha1_int(value: str, bits: int = 160) -> int:
    h = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return int(h, 16) % (2 ** bits)


class DFSException(Exception):
    pass


class FileAlreadyExistsDFS(DFSException):
    pass


class FileNotFoundDFS(DFSException):
    pass


class CorruptFileDFS(DFSException):
    pass


class PaxosCommitFailed(DFSException):
    pass


class PaxosReplica:
    def __init__(self, replica_name: str):
        self.replica_name = replica_name
        self.accepted: Dict[int, Any] = {}
        self.learned: Dict[int, Any] = {}
        self.log: List[tuple] = []

    def on_accept(self, proposal_no: int, operation: Dict[str, Any]) -> bool:
        print(f"[PAXOS][{self.replica_name}] ACCEPT({proposal_no}, {operation['op']})")
        self.accepted[proposal_no] = operation
        return True

    def on_learn(self, proposal_no: int, operation: Dict[str, Any]) -> bool:
        print(f"[PAXOS][{self.replica_name}] LEARN({proposal_no}, {operation['op']})")
        self.learned[proposal_no] = operation
        self.log.append((proposal_no, operation))
        return True


class PaxosCluster:
    def __init__(self, replicas: List[PaxosReplica]):
        if len(replicas) < 3:
            raise ValueError("Need at least 3 replicas for majority-based commitment")
        self.replicas = replicas
        self.leader = replicas[0]
        self.proposal_no = 0
        self.committed: List[tuple] = []

    def commit(self, operation: Dict[str, Any]) -> bool:
        self.proposal_no += 1
        t = self.proposal_no
        majority = len(self.replicas) // 2 + 1

        print(f"[PAXOS][LEADER={self.leader.replica_name}] PROPOSE({t}, {operation})")

        accepted_replicas = []
        for replica in self.replicas:
            ok = replica.on_accept(t, operation)
            if ok:
                accepted_replicas.append(replica)

        learned_count = 0
        for replica in accepted_replicas:
            ok = replica.on_learn(t, operation)
            if ok:
                learned_count += 1

        if learned_count >= majority:
            self.committed.append((t, operation))
            print(f"[PAXOS][LEADER={self.leader.replica_name}] COMMIT({t}, {operation['op']})")
            return True

        print(f"[PAXOS][LEADER={self.leader.replica_name}] ABORT({t}, {operation['op']})")
        return False

    def dump_logs(self) -> None:
        print("\n=== PAXOS LOGS ===")
        for replica in self.replicas:
            print(f"Replica {replica.replica_name}:")
            for proposal_no, operation in replica.log:
                print(f"  t={proposal_no} op={operation}")
        print("==================\n")


class ChordNode:
    # Minimal Chord-like peer. Stores keys that map to it as the successor node on the ring.

    def __init__(self, node_name: str, m_bits: int = 8):
        self.node_name = node_name
        self.m_bits = m_bits
        self.ring_size = 2 ** m_bits
        self.node_id = sha1_int(node_name, bits=m_bits)

        self.successor: Optional["ChordNode"] = None
        self.predecessor: Optional["ChordNode"] = None

        self.data_store: Dict[int, str] = {}
        self.pending: Dict[str, List[tuple]]={}

    def __repr__(self) -> str:
        return f"ChordNode(name={self.node_name}, id={self.node_id})"

    def put_local(self, key: int, value: str) -> None:
        self.data_store[key] = value

    def get_local(self, key: int) -> Optional[str]:
        return self.data_store.get(key)

    def delete_local(self, key: int) -> None:
        if key in self.data_store:
            del self.data_store[key]

    def recv_record(self, job: str, sort_val, raw_key: str, payload: str) -> None:
        if job not in self.pending:
            self.pending[job] = []
        self.pending[job].append((sort_val, raw_key, payload))

    def local_sort(self, job: str) -> None:
        if job in self.pending:
            self.pending[job].sort(key=lambda entry: (entry[0], entry[2]))

    def get_records(self, job: str) -> List[tuple]:
        return list(self.pending.get(job, []))

    def drop_job(self, job: str) -> None:
        if job in self.pending:
            del self.pending[job]


class ChordRing:
    # Simplified Chord ring manager. This handles successor lookup by sorting node IDs.

    def __init__(self, m_bits: int = 8):
        self.m_bits = m_bits
        self.ring_size = 2 ** m_bits
        self.nodes: List[ChordNode] = []

    def add_node(self, node_name: str) -> ChordNode:
        node = ChordNode(node_name, self.m_bits)

        existing_ids = {n.node_id for n in self.nodes}
        if node.node_id in existing_ids:
            raise ValueError(
                f"Hash collision for node '{node_name}' with id {node.node_id}. "
                "Choose a different name or increase m_bits."
            )

        self.nodes.append(node)
        self.nodes.sort(key=lambda n: n.node_id)
        self._update_links()
        self._rebalance_keys()
        return node

    def _update_links(self) -> None:
        if not self.nodes:
            return

        for i, node in enumerate(self.nodes):
            node.successor = self.nodes[(i + 1) % len(self.nodes)]
            node.predecessor = self.nodes[(i - 1) % len(self.nodes)]

    def _rebalance_keys(self) -> None:
        #Reassign keys to their correct owners after topology change.
        if not self.nodes:
            return

        all_items: Dict[int, str] = {}
        for node in self.nodes:
            all_items.update(node.data_store)
            node.data_store = {}

        for key, value in all_items.items():
            owner = self.locate_successor(key)
            owner.put_local(key, value)

    def locate_successor(self, key: int) -> ChordNode:
        if not self.nodes:
            raise ValueError("Ring has no nodes")

        node_ids = [n.node_id for n in self.nodes]
        idx = bisect_left(node_ids, key)
        if idx == len(self.nodes):
            idx = 0
        return self.nodes[idx]

    def put(self, key: int, value: str) -> None:
        owner = self.locate_successor(key)
        owner.put_local(key, value)

    def get(self, key: int) -> Optional[str]:
        owner = self.locate_successor(key)
        return owner.get_local(key)

    def delete(self, key: int) -> None:
        owner = self.locate_successor(key)
        owner.delete_local(key)

    def dump_ring(self) -> None:
        print("\n=== CHORD RING ===")
        for node in self.nodes:
            pred_id = node.predecessor.node_id if node.predecessor else None
            succ_id = node.successor.node_id if node.successor else None
            print(
                f"Node {node.node_name} | id={node.node_id} | "
                f"pred={pred_id} | succ={succ_id} | keys={sorted(node.data_store.keys())}"
            )
        print("==================\n")


class DFS:
    # DFS layer on top of the Chord ring.

    GLOBAL_INDEX_KEY_RAW = "global:file_index"

    def __init__(self, chord: ChordRing, page_size: int = 1024, m_bits: int = 8, paxos: Optional[PaxosCluster] = None):
        self.chord = chord
        self.page_size = page_size
        self.m_bits = m_bits
        self.global_index_key = sha1_int(self.GLOBAL_INDEX_KEY_RAW, bits=self.m_bits)
        self.paxos = paxos
    def _hash(self, raw: str) -> int:
        return sha1_int(raw, bits=self.m_bits)

    def _metadata_key(self, filename: str) -> int:
        return self._hash(f"metadata:{filename}")

    def _page_key(self, filename: str, page_no: int) -> int:
        return self._hash(f"{filename}:{page_no}")

    def _encode(self, obj: Any) -> str:
        return json.dumps(obj)

    def _decode(self, raw: Optional[str]) -> Optional[Any]:
        if raw is None:
            return None
        return json.loads(raw)

    def _replicate_update(self, operation: Dict[str, Any]) -> None:
        if self.paxos is None:
            return
        committed = self.paxos.commit(operation)
        if not committed:
            raise PaxosCommitFailed(f"Paxos failed to commit operation: {operation}")

    def _get_metadata(self, filename: str) -> Dict[str, Any]:
        mkey = self._metadata_key(filename)
        metadata = self._decode(self.chord.get(mkey))
        if metadata is None:
            raise FileNotFoundDFS(f"File '{filename}' not found")
        return metadata

    def _put_metadata(self, metadata: Dict[str, Any]) -> None:
        mkey = self._metadata_key(metadata["filename"])
        self.chord.put(mkey, self._encode(metadata))

    def _get_file_index(self) -> List[str]:
        raw = self.chord.get(self.global_index_key)
        obj = self._decode(raw)
        if obj is None:
            return []
        return obj["files"]

    def _put_file_index(self, files: List[str]) -> None:
        files = sorted(set(files))
        self.chord.put(self.global_index_key, self._encode({"files": files}))

    def _add_to_file_index(self, filename: str) -> None:
        files = self._get_file_index()
        if filename not in files:
            files.append(filename)
            self._put_file_index(files)

    def _remove_from_file_index(self, filename: str) -> None:
        files = self._get_file_index()
        files = [f for f in files if f != filename]
        self._put_file_index(files)

    def touch(self, filename: str) -> None:
        mkey = self._metadata_key(filename)
        if self.chord.get(mkey) is not None:
            raise FileAlreadyExistsDFS(f"File '{filename}' already exists")
        self._replicate_update({"op": "touch", "filename": filename})

        metadata = {
            "filename": filename,
            "size_bytes": 0,
            "num_pages": 0,
            "pages": [],
            "version": 1
        }

        self.chord.put(mkey, self._encode(metadata))
        self._add_to_file_index(filename)

    def append(self, filename: str, local_path: str) -> None:
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file '{local_path}' does not exist")

        metadata = self._get_metadata(filename)

        with open(local_path, "rb") as f:
            data = f.read()

        if not data:
            return

        chunks = [
            data[i:i + self.page_size]
            for i in range(0, len(data), self.page_size)
        ]
        self._replicate_update({
            "op": "append",
            "filename": filename,
            "bytes": len(data),
            "new_pages": len(chunks),
            "starting_page_no": metadata["num_pages"]
        })

        for chunk in chunks:
            page_no = metadata["num_pages"]
            pkey = self._page_key(filename, page_no)

            page_obj = {
                "filename": filename,
                "page_no": page_no,
                "size_bytes": len(chunk),
                "data": chunk.decode("latin1")
            }

            self.chord.put(pkey, self._encode(page_obj))

            metadata["pages"].append({
                "page_no": page_no,
                "guid": pkey
            })
            metadata["num_pages"] += 1
            metadata["size_bytes"] += len(chunk)

        metadata["version"] += 1
        self._put_metadata(metadata)

    def read(self, filename: str) -> bytes:
        metadata = self._get_metadata(filename)
        output = bytearray()

        for desc in sorted(metadata["pages"], key=lambda x: x["page_no"]):
            pkey = desc["guid"]
            page_obj = self._decode(self.chord.get(pkey))
            if page_obj is None:
                raise CorruptFileDFS(
                    f"Missing page {desc['page_no']} for file '{filename}'"
                )
            output.extend(page_obj["data"].encode("latin1"))

        return bytes(output)

    def read_text(self, filename: str, encoding: str = "utf-8") -> str:
        return self.read(filename).decode(encoding, errors="replace")

    def head(self, filename: str, n: int, by_lines: bool = False) -> bytes:
        data = self.read(filename)

        if by_lines:
            text = data.decode("utf-8", errors="replace")
            lines = text.splitlines(keepends=True)
            return "".join(lines[:n]).encode("utf-8")

        return data[:n]

    def tail(self, filename: str, n: int, by_lines: bool = False) -> bytes:
        data = self.read(filename)

        if by_lines:
            text = data.decode("utf-8", errors="replace")
            lines = text.splitlines(keepends=True)
            return "".join(lines[-n:]).encode("utf-8")

        if n <= 0:
            return b""
        return data[-n:]

    def delete_file(self, filename: str) -> None:
        metadata = self._get_metadata(filename)

        self._replicate_update({
            "op": "delete_file",
            "filename": filename,
            "pages": metadata["num_pages"]
        })

        for desc in metadata["pages"]:
            self.chord.delete(desc["guid"])

        self.chord.delete(self._metadata_key(filename))
        self._remove_from_file_index(filename)

    def ls(self) -> List[str]:
        return self._get_file_index()

    def stat(self, filename: str) -> Dict[str, Any]:
        return self._get_metadata(filename)

    def debug_where_is_metadata(self, filename: str) -> ChordNode:
        return self.chord.locate_successor(self._metadata_key(filename))

    def debug_where_is_page(self, filename: str, page_no: int) -> ChordNode:
        return self.chord.locate_successor(self._page_key(filename, page_no))

    def _parse_csv(self, text: str) -> List[tuple]:
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(",", 1)
            if len(parts) != 2:
                raise DFSException(f"bad line: '{line}'")
            rec_key = parts[0].strip()
            rec_val = parts[1].strip()
            try:
                sort_val = int(rec_key)
            except ValueError:
                sort_val = rec_key
            rows.append((sort_val, rec_key, rec_val))
        return rows

    def _save_to_dfs(self, fname: str, text: str) -> None:
        tmp = f"_tmp_{fname.replace('/', '_')}"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        try:
            self.touch(fname)
            if text:
                self.append(fname, tmp)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def sort_file(self, filename: str, output_filename: str) -> None:
        content = self.read_text(filename)
        rows = self._parse_csv(content)
        job = f"{filename}_{output_filename}"

        for node in self.chord.nodes:
            node.drop_job(job)

        # route each record to the node responsible for hash(key)
        for sort_val, rec_key, rec_val in rows:
            hashed_key = self._hash(rec_key)
            target_node = self.chord.locate_successor(hashed_key)
            target_node.recv_record(job, sort_val, rec_key, rec_val)

        # each node sorts its own chunk locally
        all_records = []
        for node in self.chord.nodes:
            node.local_sort(job)
            all_records.extend(node.get_records(job))

        # merge into final sorted output
        all_records.sort(key=lambda entry: (entry[0], entry[2]))

        out = "\n".join(f"{rec_key},{rec_val}" for _, rec_key, rec_val in all_records)
        if out:
            out += "\n"

        self._replicate_update({
            "op": "sort_file",
            "input": filename,
            "output": output_filename,
            "records": len(all_records)
        })

        self._save_to_dfs(output_filename, out)

        for node in self.chord.nodes:
            node.drop_job(job)


def demo():
    ring = ChordRing(m_bits=8)

    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")
    ring.add_node("node4")
    ring.add_node("node5")

    replicas = [
        PaxosReplica("replica1"),
        PaxosReplica("replica2"),
        PaxosReplica("replica3")
    ]
    paxos = PaxosCluster(replicas)

    dfs = DFS(ring, page_size=16, m_bits=8, paxos=paxos)
    with open("sample_input.txt", "w", encoding="utf-8") as f:
        f.write(
            "this is a test\n"
            "this project is for the cecs 327 class\n"
            "we are storing pages over chord\n"
            "last line here\n"
        )

    dfs.touch("report.txt")
    dfs.append("report.txt", "sample_input.txt")

    print("FILES:", dfs.ls())
    print("\nSTAT(report.txt):")
    print(json.dumps(dfs.stat("report.txt"), indent=2))

    print("\nFULL READ:")
    print(dfs.read_text("report.txt"))

    print("\nHEAD 20 BYTES:")
    print(dfs.head("report.txt", 20).decode("utf-8", errors="replace"))

    print("\nTAIL 2 LINES:")
    print(dfs.tail("report.txt", 2, by_lines=True).decode("utf-8", errors="replace"))

    owner = dfs.debug_where_is_metadata("report.txt")
    print(f"Metadata for report.txt is stored at node {owner.node_name} (id={owner.node_id})")

    page0_owner = dfs.debug_where_is_page("report.txt", 0)
    print(f"Page 0 for report.txt is stored at node {page0_owner.node_name} (id={page0_owner.node_id})")

    ring.dump_ring()

    dfs.delete_file("report.txt")
    print("FILES AFTER DELETE:", dfs.ls())

    ring.dump_ring()
    with open("records_input.txt", "w", encoding="utf-8") as f:
        f.write(
            "50,z\n"
            "10,a\n"
            "30,x\n"
            "20,b\n"
            "30,alpha\n"
        )

    dfs.touch("records.csv")
    dfs.append("records.csv", "records_input.txt")

    print("UNSORTED INPUT:")
    print(dfs.read_text("records.csv"))

    dfs.sort_file("records.csv", "records_sorted.csv")

    print("SORTED OUTPUT:")
    print(dfs.read_text("records_sorted.csv"))

    print("STAT(records_sorted.csv):")
    print(json.dumps(dfs.stat("records_sorted.csv"), indent=2))

    # quick check that output is actually sorted
    sorted_text = dfs.read_text("records_sorted.csv")
    keys = [int(line.split(",")[0]) for line in sorted_text.strip().splitlines()]
    assert keys == sorted(keys), "FAIL: not sorted"
    print("\ncorrectness check passed, keys:", keys)
    paxos.dump_logs()  # PAXOS - add this



if __name__ == "__main__":
    demo()