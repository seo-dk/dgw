#!/bin/bash

HDFS_CMD="hdfs dfs"
LOCAL_PATH="$1"
HDFS_PATH="$2"

validate_arguments() {
    if [[ -z "$LOCAL_PATH" || -z "$HDFS_PATH" ]]; then
        printf "Usage: %s <local_path> <hdfs_path>\n" "$0" >&2
        return 1
    fi
    if [[ ! -f "$LOCAL_PATH" ]]; then
        printf "Local file does not exist or is not a regular file : %s\n" "$LOCAL_PATH" >&2
    fi
}

hdfs_file_exists() {
    local path="$1"
    if ! output=$($HDFS_CMD -ls "$path" 2>/dev/null); then
        return 1
    fi

    if [[ -z "$output" || "$output" =~ "No such file or directory" ]]; then
        return 1
    fi

    return 0
}

put_to_hdfs() {
    local src="$1"
    local dst_dir="$2"
    local filename; filename=$(basename "$src")
    local dst_path="$dst/$filename"

    if hdfs_file_exists "$dst_path"; then
        printf "File alread exists in HDFS: %s\n" "$dst_dir"
        return 2
    fi

    if ! $HDFS_CMD -put "$src" "$dst_dir"; then
        printf "Failed to put file to HDFS: %s -> %s\n" "$src" "$dst_dir" >&2
        return 1
    fi
    printf "Successfully put file to HDFS: %s -> %s\n" "$src" "$dst_dir"
    return 0
}

main() {
    if ! validate_arguments; then
        return 1
    fi

    local src="$LOCAL_PATH"
    local dst="$HDFS_PATH"
    
    if ! put_to_hdfs "$src" "$dst"; then
        local code=$?
        if [[ $code -eq 1 ]]; then
          printf "error : error during hdfs put\n" >&2
          return 1
        elif [[ $code -eq 2 ]]; then
          return 2
        else
          return 0
        fi
    fi
}

main "$@"