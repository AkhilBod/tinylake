"""Rebuild the table state at a version by replaying the log."""

from dataclasses import dataclass, field

from . import log


@dataclass
class Snapshot:
    version: int
    schema: list  # [[name, type], ...]
    files: dict = field(default_factory=dict)  # path -> add action

    def num_rows(self):
        return sum(f["rows"] for f in self.files.values())

    def to_state(self):
        return {"version": self.version, "schema": self.schema, "files": list(self.files.values())}


def apply(snapshot, actions):
    for action in actions:
        if "metaData" in action:
            snapshot.schema = action["metaData"]["schema"]
        elif "add" in action:
            snapshot.files[action["add"]["path"]] = action["add"]
        elif "remove" in action:
            snapshot.files.pop(action["remove"]["path"], None)


def load(root, version=None):
    latest = log.latest_version(root)
    if latest < 0:
        raise FileNotFoundError(f"no table at {root}")
    if version is None:
        version = latest
    if not 0 <= version <= latest:
        raise ValueError(f"version {version} does not exist (latest is {latest})")

    start = log.latest_checkpoint(root, version)
    if start >= 0:
        state = log.read_checkpoint(root, start)
        snap = Snapshot(start, state["schema"], {f["path"]: f for f in state["files"]})
    else:
        snap = Snapshot(-1, None)

    for v in range(start + 1, version + 1):
        apply(snap, log.read_commit(root, v))
        snap.version = v
    return snap
