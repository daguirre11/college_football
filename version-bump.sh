#!/bin/bash
# Bumps the semantic version of the git-project.
# Inspired from https://github.com/tomologic/bump-semver

PYTHON_PACKAGE_FOLDER="nmr_easy"

semver_regex="^([0-9]+)\.([0-9]+)\.([0-9]+)$"

find_latest_semver() {
  head -n 1 VERSION | tr -d '\r'
}

increment_ver() {
  readonly bump_type="$1"
  readonly current_version="$(find_latest_semver)"
  local new_major
  local new_minor
  local new_patch
  new_major="$(echo -n "${current_version}" | sed -nr "s/${semver_regex}/\1/p")"
  new_minor="$(echo -n "${current_version}" | sed -nr "s/${semver_regex}/\2/p")"
  new_patch="$(echo -n "${current_version}" | sed -nr "s/${semver_regex}/\3/p")"

  case $bump_type in
    major)
      new_major=$((new_major + 1))
      new_minor=0
      new_patch=0
      ;;
    minor)
      new_minor=$((new_minor + 1))
      new_patch=0
      ;;
    patch)
      new_patch=$((new_patch + 1))
      ;;
  esac

  echo -n "${new_major}.${new_minor}.${new_patch}"
}

update_files() {
  readonly new_version="$1";shift
  echo "${new_version}" > VERSION
  git add VERSION
  sed "s/VERSION/${new_version}/g" version.template > ${PYTHON_PACKAGE_FOLDER}/version.py
  git add ${PYTHON_PACKAGE_FOLDER}/version.py
  sed -E "s/.+#autotag/version = \"${new_version}\" #autotag/" pyproject.toml > pyproject.toml.new
  mv -f pyproject.toml.new pyproject.toml
  git add pyproject.toml
}

bump() {
  readonly next_ver="$(increment_ver "$1")"
  readonly latest_ver="$(find_latest_semver)"
  echo "Bumping version from '${latest_ver}' to '${next_ver}'"
  update_files "${next_ver}"
  uv lock --no-upgrade
  uv sync --no-upgrade
  git add uv.lock
  git diff
  git status
  echo
  echo "Bumped to '${next_ver}'"
  echo "=> Remember to commit the changed files <="
  echo
}

usage() {
  echo "Usage: ./version-bump.sh {major|minor|patch} | -l"
  echo "Bumps the semantic version field by one for a git-project."
  echo
  echo "Example:"
  echo "  ./version-bump.sh minor"
  echo
  echo "Options:"
  echo "  -l  show the current version instead of bumping."
  exit 1
}

while getopts :l opt; do
  case $opt in
    l) LIST=1;;
    \?) usage;;
    :) echo "option -$OPTARG requires an argument"; exit 1;;
  esac
done
shift $((OPTIND-1))

if [ -n "$LIST" ];then
  find_latest_semver
  exit 0
fi

bump "$1"
