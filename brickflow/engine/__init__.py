import subprocess


def _call(cmd, **kwargs):
    return subprocess.check_output(
        [
            cmd,
        ],
        **kwargs,
    )


def is_git_dirty():
    p = _call("git diff --stat", shell=True)
    if len(p) > 10:
        return True
    return False


def get_current_branch():
    p = _call("git rev-parse --abbrev-ref HEAD", shell=True)
    return p.strip().decode("utf-8")


def get_current_commit():
    p = _call('git log -n 1 --pretty=format:"%H"', shell=True)
    return p.strip().decode("utf-8")


ROOT_NODE = "root"
