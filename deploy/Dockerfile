FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/server /out/worker /out/producer /
EXPOSE 2112 50051
ENTRYPOINT ["/server"]
