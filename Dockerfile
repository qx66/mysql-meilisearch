FROM registry.cn-hangzhou.aliyuncs.com/startops-base/golang-builder:1.20 AS builder

WORKDIR /go/src
ADD . /go/src

RUN GOPROXY=https://goproxy.cn;make build

#FROM docker.io/library/busybox:stable-glibc
FROM registry.cn-hangzhou.aliyuncs.com/startops-base/debian-runtime:11.7-slim

COPY --from=builder /go/src/bin /app

WORKDIR /app

CMD ["/app/mysql-meilisearch-linux"]
