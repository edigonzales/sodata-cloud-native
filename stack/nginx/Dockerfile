FROM nginx:mainline-alpine

COPY nginx.conf /etc/nginx/nginx.conf

RUN touch /var/run/nginx.pid && \
  chgrp -R 0 /var/run/nginx.pid && \
  chgrp -R 0 /var/cache/nginx && \
  chmod g=u /var/run/nginx.pid && \
  chmod -R g=u /var/cache/nginx

RUN mkdir -p /opt/data/

USER 11200
