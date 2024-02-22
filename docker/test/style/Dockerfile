# docker build -t clickhouse/style-test .
FROM docker.nju.edu.cn/ubuntu:22.04
ARG ACT_VERSION=0.2.33
ARG ACTIONLINT_VERSION=1.6.22

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    aspell \
    curl \
    git \
    file \
    libxml2-utils \
    moreutils \
    python3-fuzzywuzzy \
    python3-pip \
    shellcheck \
    yamllint \
    locales \
    && pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple black==23.1.0 boto3 codespell==2.2.1 mypy==1.3.0 PyGithub unidiff pylint==2.6.2 \
    && apt-get clean \
    && rm -rf /root/.cache/pip 

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Get act and actionlint from releases
COPY act_Linux_x86_64.tar.gz /
COPY actionlint_1.6.22_linux_amd64.tar.gz /
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) act_arch=x86_64 ;; \
      arm64) act_arch=$arch ;; \
    esac \
  && mv /act_Linux_x86_64.tar.gz /tmp/act.tgz \
  && tar xf /tmp/act.tgz -C /usr/bin act \
  && rm /tmp/act.tgz \
  && mv actionlint_1.6.22_linux_amd64.tar.gz /tmp/actiolint.zip \
#  && curl -o /tmp/actiolint.zip -L \
#    "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_${arch}.tar.gz" \
  && tar xf /tmp/actiolint.zip -C /usr/bin actionlint \
  && rm /tmp/actiolint.zip


COPY run.sh /
COPY process_style_check_result.py /
CMD ["/bin/bash", "/run.sh"]
