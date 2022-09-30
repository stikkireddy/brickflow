import subprocess
from enum import Enum


class GitState(Enum):
    dirty = "dirty"
    clean = "clean"


def is_git_dirty():
    p = subprocess.check_output(['git diff --stat', ], shell=True)
    if len(p) > 10:
        return GitState.dirty
    return GitState.clean
