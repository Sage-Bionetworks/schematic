ARG imageVersion=bullseye-20221004-slim
FROM debian:$imageVersion

# Safer bash scripts with 'set -euxo pipefail'
SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]

# https://github.com/yarnpkg/berry
ARG yarnVersion="3.3.0"
# https://www.npmjs.com/package/@devcontainers/cli
ARG devcontainerCliVersion="0.25.2"
# https://pypi.org/project/poetry
ARG poetryVersion="1.2.0rc1"

# Create the docker group so that we can assign it to the user.
# This is to enable the non-root user to use the command `docker`.
# hadolint ignore=DL3008,DL3013
RUN groupadd docker \
  && apt-get update -qq -y && export DEBIAN_FRONTEND=noninteractive \
  && apt-get install --no-install-recommends -qq -y \
    ca-certificates curl git bash-completion gnupg2 lsb-release ssh sudo \
    python3-pip python-is-python3 openjdk-17-jdk \
    htop unzip vim wget lsof iproute2 build-essential \
  # Add Node.js repository
  && curl -sL https://deb.nodesource.com/setup_18.x | bash - \
  # Install extra packages
  && apt-get update -qq -y \
  && apt-get install --no-install-recommends -qq -y \
    nodejs \
  && corepack enable yarn \
  && corepack prepare "yarn@${yarnVersion}" --activate \
  # Install @devcontainers/cli
  && npm install -g "@devcontainers/cli@${devcontainerCliVersion}" \
  # Cleanup
  && apt-get -y autoclean \
  && apt-get -y autoremove \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --no-cache-dir pipenv

ARG user=vscode
RUN useradd -m $user \
  && echo "$user:$user" | chpasswd \
  && usermod -a --groups docker,sudo $user \
  && usermod --shell /bin/bash $user \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && printf "%s\n" \
    "" \
    "# source dev-env.sh if found in the current directory" \
    "if [ -f dev-env.sh ]; then" \
    "  . ./dev-env.sh" \
    "  workspace-initialize-env" \
    "fi" | tee -a /home/$user/.bashrc

USER $user

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --version "${poetryVersion}"

CMD ["bash"]