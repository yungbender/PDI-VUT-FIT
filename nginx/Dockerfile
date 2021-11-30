FROM nginx:1.21.4-alpine

RUN rm /etc/nginx/nginx.conf

COPY "./src/*.js" "/pdi/src/nginx/"
COPY "./src/nginx.conf" "/etc/nginx/nginx.conf"

COPY "./change_volume_perm.sh" "/docker-entrypoint.d/"
RUN ["chmod", "+x", "/docker-entrypoint.d/change_volume_perm.sh"]
