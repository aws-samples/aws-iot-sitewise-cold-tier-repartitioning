# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os

def visible_child_dirs(dir_path: str) -> list[str]:
    """List all visible child directories exclusing hidden files 
    like .DS_Store
    """
    dirs = [x for x in os.listdir(dir_path) if not x.startswith('.')]
    return dirs