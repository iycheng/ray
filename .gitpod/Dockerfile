FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

RUN apt-get update -qq
RUN apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip apt-utils dialog tzdata wget rsync \
    language-pack-en tmux cmake gdb vim htop \
    libgtk2.0-dev zlib1g-dev libgl1-mesa-dev maven \
    openjdk-8-jre openjdk-8-jdk clang-format-7
RUN ln -s /usr/bin/clang-format-7 /usr/bin/clang-format

# System conf for tests
RUN locale -a
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
RUN echo "ulimit -c 0" >> /root/.bashrc
