FROM ubuntu:20.10
ENV SHELL=/bin/bash
ENV TERM=xterm-256color
ENV LANG=C.UTF-8
ENV LC_TERMINAL=iTerm2
ENV LC_TERMINAL_VERSION=3.4.4
ARG DEBIAN_FRONTEND=noninteractive
RUN set -x; apt update \
    && ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime \
    && apt install emacs gdb wget npm git build-essential curl unzip zip psmisc curl gnupg python3 pip iptables ycmd -y \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && apt install default-jre default-jdk clang rtags tmux clang-format shellcheck cmake autogen python-dev automake autoconf libtool -y \
    && curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg \
    && mv bazel.gpg /etc/apt/trusted.gpg.d/ \
    && echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list \
    && apt update && apt install bazel-3.7.2 -y

RUN  pip3 install cython==0.29.0 pytest pandas tree tabulate pexpect sklearn joblib yapf==0.23.0 flake8==3.7.7 mypy==0.782 flake8-quotes

RUN update-alternatives --install /usr/local/bin/python python /usr/bin/python3 30 \
    && update-alternatives --install /usr/bin/bazel bazel /usr/bin/bazel-3.7.2 30 \
    && echo "kernel.yama.ptrace_scope = 0" > /etc/sysctl.d/10-ptrace.conf
