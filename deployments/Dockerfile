# Dockerfile generico e parametrizzato: costruisce uno qualsiasi dei 3
# binari del sistema (consensus-node, proxy, snapshot) a seconda del
# build-arg SERVICE, invece di duplicare 3 Dockerfile quasi identici.
#
# Costruzione multi-stadio: lo stage "build" contiene l'intero toolchain Go
# (pesante, serve solo per compilare); lo stage finale è un'immagine minimale
# che contiene solo il binario già compilato, mai il compilatore.

FROM golang:1.26-alpine AS build

WORKDIR /src

# Le dipendenze cambiano meno spesso del codice applicativo: copiandole e
# scaricandole prima, Docker può riusare questo layer dalla cache finché
# go.mod/go.sum non cambiano, anche se il codice sorgente cambia spesso.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" || (echo "build-arg SERVICE obbligatorio (consensus-node|proxy|snapshot)" && false)
RUN CGO_ENABLED=0 go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /out/service /usr/local/bin/service

ENTRYPOINT ["/usr/local/bin/service"]
