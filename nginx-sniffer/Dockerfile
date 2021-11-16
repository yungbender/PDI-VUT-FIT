FROM nginx:1.21.4-alpine

RUN mkdir "/tmp/nginx_logs" && \ 
    chmod 777 "/tmp/nginx_logs"
RUN rm /etc/nginx/nginx.conf

RUN apk add --update nodejs npm

COPY "./src/nginx/nginx.conf" "/etc/nginx/nginx.conf"

EXPOSE 8000
