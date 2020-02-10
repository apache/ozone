#!/usr/bin/env bash
#doc: add new empty commit to trigger new CI build
git config --global user.email "noreply@github.com"
git config --global user.name "GitHub"
git commit --allow-empty -m "retest build"
git push origin
