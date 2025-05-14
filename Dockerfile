# Use Astro Runtime 12.9.0 (or upgrade to 3.0-2 if needed)
FROM quay.io/astronomer/astro-runtime:12.9.0

# Switch to root user to install packages
USER root

# Install bash
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Switch back to astro user for security
USER astro

