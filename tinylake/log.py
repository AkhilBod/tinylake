"""The transaction log.

A table is a directory of Parquet files plus a `_log/` directory. Version N of
the table is described by commit file `_log/<N, zero-padded>.json`, one JSON
action per line. Replaying commits 0..N gives the set of live data files at
version N.

The only primitive the log needs from storage is "create this file only if it
does not already exist". Two writers that both try to commit version N race on
that create; exactly one wins.
"""

import json
import os
import tempfile

LOG_DIR = "_log"
CHECKPOINT_EVERY = 10


class ConcurrentModificationError(Exception):
    """Another writer committed a change this transaction cannot be rebased over."""


def log_dir(root):
    return os.path.join(root, LOG_DIR)


def commit_path(root, version):
    return os.path.join(log_dir(root), f"{version:020d}.json")


def checkpoint_path(root, version):
    return os.path.join(log_dir(root), f"{version:020d}.checkpoint.json")


def _write_new(path, text):
    """Atomically create `path` with `text`. Returns False if it already exists.

    Write to a temp file first, then hard-link it into place. link() fails if
    the target exists, and readers never see a half-written commit.
    """
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tmp-")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            return False
        return True
    finally:
        os.unlink(tmp)


def write_commit(root, version, actions):
    text = "".join(json.dumps(a, sort_keys=True) + "\n" for a in actions)
    return _write_new(commit_path(root, version), text)


def read_commit(root, version):
    with open(commit_path(root, version)) as f:
        return [json.loads(line) for line in f if line.strip()]


def list_versions(root):
    versions = []
    for name in os.listdir(log_dir(root)):
        stem, _, ext = name.partition(".")
        if ext == "json" and stem.isdigit():
            versions.append(int(stem))
    return sorted(versions)


def latest_version(root):
    versions = list_versions(root)
    return versions[-1] if versions else -1


def latest_checkpoint(root, at_or_before):
    best = -1
    for name in os.listdir(log_dir(root)):
        if name.endswith(".checkpoint.json"):
            v = int(name.split(".")[0])
            if best < v <= at_or_before:
                best = v
    return best


def write_checkpoint(root, version, state):
    """Checkpoints are an optimization, so any writer may (re)write one."""
    path = checkpoint_path(root, version)
    fd, tmp = tempfile.mkstemp(dir=log_dir(root), prefix=".tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(state, f, sort_keys=True)
    os.replace(tmp, path)


def read_checkpoint(root, version):
    with open(checkpoint_path(root, version)) as f:
        return json.load(f)
