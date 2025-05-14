FROM quay.io/astronomer/astro-runtime:12.9.0

# Switch to root to install packages
USER root

# Install bash explicitly if it's not already available
RUN apt-get update && \
    apt-get install -y bash && \
    rm -rf /var/lib/apt/lists/*

# (Optional) Set bash as the default shell
SHELL ["/bin/bash", "-c"]

# Switch back to the astro user
USER astro
