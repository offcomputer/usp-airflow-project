#!/bin/bash

if git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
  ROOT=$(git rev-parse --show-toplevel)
else
  echo "Warning: Not inside a Git repository. Using current directory."
  ROOT=$(pwd)
fi

cd "$ROOT" || exit 1

echo '```'

find . -path ./.git -prune -o -print | sort | while read -r line; do
  depth=$(echo "$line" | grep -o "/" | wc -l)
  indent=""
  if [ "$depth" -gt 1 ]; then
    indent=$(printf '│   %.0s' $(seq 1 $((depth - 1))))
  fi
  connector="├──"
  echo "$indent$connector $(basename "$line")"
done

echo '```'
