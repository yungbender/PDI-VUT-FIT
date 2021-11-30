FROM alpine:3.15

RUN apk add --update nodejs npm make

COPY "./Makefile" "/pdi/"
COPY "./*.json" "/pdi/"
COPY "./src/*.ts" "/pdi/src/"

WORKDIR /pdi

RUN make install 
RUN make compile

