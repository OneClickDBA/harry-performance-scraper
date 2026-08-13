ARG BASE_IMAGE
FROM ${BASE_IMAGE:-ghcr.io/oracle/oraclelinux:8-slim} AS build-deps

ARG TARGETOS
ARG GOOS=${TARGETOS}
ENV GOOS=${GOOS:-linux}

ARG TARGETARCH
ARG GOARCH=${TARGETARCH}
ENV GOARCH=${GOARCH:-amd64}

ARG TAGS
ENV TAGS=${TAGS:-godror}

ARG CGO_ENABLED
ENV CGO_ENABLED=${CGO_ENABLED:-1}

ARG GO_VERSION=1.26.3
ENV GO_VERSION=${GO_VERSION}

RUN microdnf update -y && \
    microdnf install -y wget gzip gcc jq && \
    microdnf clean all && \
    go_tarball="go${GO_VERSION}.${GOOS}-${GOARCH}.tar.gz" && \
    wget -q "https://go.dev/dl/${go_tarball}" && \
    go_checksum="$(wget -qO- 'https://go.dev/dl/?mode=json&include=all' | jq -r --arg version "go${GO_VERSION}" --arg os "${GOOS}" --arg arch "${GOARCH}" '.[] | select(.version == $version) | .files[] | select(.os == $os and .arch == $arch) | .sha256' | head -n 1)" && \
    test -n "${go_checksum}" && test "${go_checksum}" != "null" && \
    printf '%s  %s\n' "${go_checksum}" "${go_tarball}" | sha256sum -c - && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf "${go_tarball}" && \
    rm "${go_tarball}"

ENV PATH=$PATH:/usr/local/go/bin

WORKDIR /go/src/harry-scraper
COPY go.mod go.sum ./
RUN go mod download

FROM build-deps AS build

ARG TARGETOS
ARG GOOS=${TARGETOS}
ENV GOOS=${GOOS:-linux}

ARG TARGETARCH
ARG GOARCH=${TARGETARCH}
ENV GOARCH=${GOARCH:-amd64}

ARG TAGS
ENV TAGS=${TAGS:-godror}

ARG CGO_ENABLED
ENV CGO_ENABLED=${CGO_ENABLED:-1}

COPY . .

ARG VERSION
ENV VERSION=${VERSION:-0.0.0-dev}

RUN CGO_ENABLED=${CGO_ENABLED} GOOS=${GOOS} GOARCH=${GOARCH} go build --tags=${TAGS} -v -ldflags "-X main.Version=${VERSION} -s -w" -o /tmp/harry-scraper

FROM scratch AS release-binary

COPY --from=build /tmp/harry-scraper /harry-scraper

FROM ${BASE_IMAGE:-ghcr.io/oracle/oraclelinux:8-slim} AS scraper-godror

ARG VERSION

LABEL org.opencontainers.image.title="Harry"
LABEL org.opencontainers.image.description="Harry — Performance Scraper for Oracle Database"
LABEL org.opencontainers.image.authors="Jorge Holgado <dodger@oneclickdba.com>"
LABEL org.opencontainers.image.vendor="Jorge Holgado"
LABEL org.opencontainers.image.licenses="UPL-1.0 AND MIT"
LABEL org.opencontainers.image.source="https://github.com/OneClickDBA/harry-performance-scraper"
LABEL org.opencontainers.image.documentation="https://oneclickdba.github.io/harry-performance-scraper-web/"
LABEL org.opencontainers.image.url="https://oneclickdba.com/harry/"
LABEL org.opencontainers.image.version="${VERSION:-0.0.0-dev}"

ENV VERSION=${VERSION:-0.0.0-dev}
ENV DEBIAN_FRONTEND=noninteractive

ARG ORACLE_INSTANTCLIENT_RELEASE=26ai

RUN microdnf update -y && \
    oracle_linux_major="$(. /etc/os-release; printf '%s' "${VERSION_ID%%.*}")" && \
    microdnf install -y "oracle-instantclient-release-${ORACLE_INSTANTCLIENT_RELEASE}-el${oracle_linux_major}" && \
    microdnf install -y oracle-instantclient-basic glibc && \
    microdnf clean all

COPY --from=build /tmp/harry-scraper /harry-scraper

# create the mount point for alert log exports (default location)
RUN mkdir /log && chown 1000:1000 /log
RUN mkdir /wallet && chown 1000:1000 /wallet

EXPOSE 9161

USER 1000

ENTRYPOINT ["/harry-scraper"]

FROM ${BASE_IMAGE:-ghcr.io/oracle/oraclelinux:8-slim} AS scraper-goora

ARG VERSION

LABEL org.opencontainers.image.title="Harry"
LABEL org.opencontainers.image.description="Harry — Performance Scraper for Oracle Database"
LABEL org.opencontainers.image.authors="Jorge Holgado <dodger@oneclickdba.com>"
LABEL org.opencontainers.image.vendor="Jorge Holgado"
LABEL org.opencontainers.image.licenses="UPL-1.0 AND MIT"
LABEL org.opencontainers.image.source="https://github.com/OneClickDBA/harry-performance-scraper"
LABEL org.opencontainers.image.documentation="https://oneclickdba.github.io/harry-performance-scraper-web/"
LABEL org.opencontainers.image.url="https://oneclickdba.com/harry/"
LABEL org.opencontainers.image.version="${VERSION:-0.0.0-dev}"

ENV VERSION=${VERSION:-0.0.0-dev}

COPY --from=build /tmp/harry-scraper /harry-scraper

# create the mount point for alert log exports (default location)
RUN mkdir /log && chown 1000:1000 /log
RUN mkdir /wallet && chown 1000:1000 /wallet

EXPOSE 9161

USER 1000

ENTRYPOINT ["/harry-scraper"]
