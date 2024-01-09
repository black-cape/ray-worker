#!/usr/bin/env bash
set -e

VALID_BUMP_RULES=("patch", "minor", "major")
VALID_PROJECT_TYPES=("python", "node", "plain")

function get_python_version {
  local version_file=$1
  local current_version=`grep -m 1 version $version_file | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3`
  echo "$current_version"
}

function set_python_version {
  local new_version=$1
  local version_file=$2
  poetry version $new_version
}

function get_node_version {
  local version_file=$1
  local current_version=`node -p "require('$version_file').version"`
  echo "$current_version"
}

function set_node_version {
  local new_version=$1
  local version_file=$2
  npm --no-git-tag-version --force version $new_version
}

function get_text_version {
  local version_file=$1
  local current_version=`cat $version_file`
  echo "$current_version"
}

function set_text_version {
  local new_version=$1
  local version_file=$2
  echo "$new_version" > $version_file
}

function get_version {
  local project_type=$1
  local version_file=$2
  case $project_type in
    python)
      local current_version=$(get_python_version "$version_file")
      ;;
    node)
      local current_version=$(get_node_version "$version_file")
      ;;
    plain)
      local current_version=$(get_text_version "$version_file")
      ;;
    *)
      echo "Invalid project type $project_type. Must be one of the following: python, node, plain."
      exit 1
      ;;
  esac
  echo "$current_version"
}

function bump_version {
  local bump_type=$1
  local project_type=$2
  local version_file=$3
  local scripts_command="${4:='./'}semver.sh"

  case $bump_type in
    patch|minor|major)
      echo "Bumping project $bump_type version."
      ;;
    *)
      echo "Invalid bump rule $bump_type. Must be one of the following: patch, minor, major."
      exit 1
      ;;
  esac

  if [ -z "${version_file}" ]; then echo "File target is not set " && exit; fi

  local current_version=$(get_version "$project_type" "$version_file")
  echo "current version is $current_version"
  local new_version=`bash -c "$scripts_command bump $bump_type $current_version"`
  echo "new version is $new_version"
  case $project_type in
    python)
      set_python_version "$new_version" "$version_file"
      ;;
    node)
      set_node_version "$new_version" "$version_file"
      git add package-lock.json
      ;;
    plain)
      set_text_version "$new_version" "$version_file"
      ;;
    *)
      echo "Invalid project type $project_type. Must be one of the following: python, node, plain."
      exit 1
      ;;
  esac
  git add "$version_file"
  git commit -m "Bump project version to $new_version"
  echo "Creating a tag for version $new_version"
  git tag $new_version
  git push origin $new_version || {
    echo "Failed to push tag $new_version. Exiting.";
    exit 1;
  }
  echo "Pushing commits to remote"
  git push
}

"$@"
