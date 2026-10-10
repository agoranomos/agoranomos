# The proxy of server mode (ADR-0019): the official image of Caddy, with one change. Its binary
# carries the file capability cap_net_bind_service, and the kernel refuses to run a binary whose
# file capability is not in the bounding set of the process: with every capability dropped, as
# every service of the stack runs, Caddy would not start. Docker lets a container bind the ports
# below 1024 without that capability (since Docker 20.10), so it is removed from the file.
# The Caddyfile is part of the image: Caddy runs with its admin API off and reads its configuration
# only when it starts, and ./agoranomos up builds this image on every start, so a changed Caddyfile
# makes a new image, and Compose recreates the proxy. The image is never pushed to a registry.
FROM caddy:2.11.7-alpine@sha256:d8542f48d34a9cf4e4c11a478865229840e87e4c96ea3f439101f31a5d35f75f

RUN setcap -r /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
