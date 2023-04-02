#!/bin/bash

# Previously set to 128, under the belief that it should be set to the same value
# as maxFiles in gVisor (https://github.com/google/gvisor/blob/master/pkg/urpc/urpc.go#L40)
# Our experiment showed that these values are not related
# A value of 128 frequently triggered a "Too many open files" error on trajectory execution.
# Increasing this value to 1024 demonstrably improved performance.
# See: https://www.notion.so/openai/ACE-ulimit-n-file-descriptor-55554c175ac54848b6b9176b87e5d69b?pvs=4
# and https://github.com/openai/ace/pull/48
ulimit -n 1024

#ulimit -u 1024 # prevent fork bombs from wasting CPU resources
ulimit -v $PROCESS_MEMORY_LIMIT # limit process to have OOMs usually cause python error instead of kube killing a kernel
# Single-threaded because we're using a single global notebook connection.
cd $HOME/.openai_internal/ && python3 -m hypercorn --workers=1 --bind=0.0.0.0:8080 user_machine.app:app
