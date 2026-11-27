FROM docker.io/library/golang:1.26.3-bookworm

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    zip \
    unzip \
    gnupg-agent \
    gnupg2 lsb-release \
    software-properties-common \
    build-essential \
    dbus-x11 xvfb && \
    rm -rf /var/lib/apt/lists/*


# google chrome
RUN \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*


# Cypress
ENV QT_X11_NO_MITSHM=1 _X11_NO_MITSHM=1 _MITSHM=0 CYPRESS_CACHE_FOLDER=/home/user/.cache/Cypress
ENV DBUS_SESSION_BUS_ADDRESS=/dev/null

ENV CI=1


# Even though we use bun, we still need node for playwright (incl. storybook)
RUN curl -fsSL https://nodejs.org/dist/v20.19.4/node-v20.19.4-linux-x64.tar.xz | tar -xJ -C /opt && \
    ln -s /opt/node-v20.19.4-linux-x64/bin/node /usr/bin/node && \
    ln -s /opt/node-v20.19.4-linux-x64/bin/npm /usr/bin/npm && \
    ln -s /opt/node-v20.19.4-linux-x64/bin/npx /usr/bin/npx


# Create a default user and switch to it
RUN useradd -m -s /bin/bash user
USER user
ENV HOME=/home/user
WORKDIR /home/user

