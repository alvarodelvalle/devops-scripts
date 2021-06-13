import os

import git
from git import Repo

def get_changed_directories():
    repo = git.Repo('/Users/avalle/development/bitbucket/policymap/build')
    tree = repo.heads.'alvaro/PR-1201-apply-terraform-configuration-fo'.commit.tree
    print(f'Git repo: {repo}')
    print(f'Git tree: {tree}')


get_changed_directories()
