#!/bin/bash

# Run git log and iterate over each line
git log --oneline 385c4ec6ca^..3e70cf4165 --reverse| while read -r line; do
    # Extract commit hash from each line
    commit_hash=$(echo "$line" | awk '{print $1}')

    echo $commit_hash
    
    # Execute git cherry-pick -x for each commit hash
    git cherry-pick -x "$commit_hash"
    git commit --amend --no-edit
    
    # Check if cherry-pick was successful
    if [ $? -ne 0 ]; then
        echo "Error cherry-picking commit $commit_hash"
        exit 1
    fi
done
