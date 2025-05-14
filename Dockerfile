FROM quay.io/astronomer/astro-runtime:12.9.0

# Switch to root to install packages
USER root

# Install bash explicitly and symlink if needed
RUN apt-get update && \
    apt-get install -y bash && \
    ln -sf /bin/bash /usr/bin/bash && \
    rm -rf /var/lib/apt/lists/*

# (Optional) Set bash as the default shell
SHELL ["/bin/bash", "-c"]

# Switch back to astro user for safety
USER astro
