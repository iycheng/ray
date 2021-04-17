startup --output_base=/workspace/ray/.bazel-cache
build --host_jvm_args=-Xmx1800m
build --jobs=6
