#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

accord_repo='git@github.com:bdeggleston/cassandra-accord.git'
accord_branch='metadata-persistence'
accord_src="$bin/cassandra-accord"

checkout() {
  cd "$accord_src"
    git checkout "$accord_branch"
    echo "$accord_branch" > .BRANCH
  cd -
}

_main() {
  # have we already cloned?
  if [[ ! -e "$accord_src" ]]; then
    git clone "$accord_repo" "$accord_src"
    checkout
  fi
  if [[ $(cat "$accord_src"/.BRANCH || true) != "$accord_branch" ]]; then
    checkout
  fi
  cd "$accord_src"
  # are there changes?
  git pull --rebase origin "$accord_branch"
  if [[ $(git rev-parse HEAD) != $(cat .SHA || true) ]]; then
    ./gradlew clean install -x test
    git rev-parse HEAD > .SHA
  fi
}

_main "$@"
