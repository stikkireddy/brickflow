import subprocess


def is_git_dirty():
    p = subprocess.check_output(['git diff --stat', ], shell=True)
    print(p)
    if len(p) > 10:
        return True
    return False


def get_current_branch():
    p = subprocess.check_output(['git rev-parse --abbrev-ref HEAD', ], shell=True)
    return p.strip().decode("utf-8")


def get_current_commit():
    p = subprocess.check_output(['git log -n 1 --pretty=format:"%H"', ], shell=True)
    return p.strip().decode("utf-8")

