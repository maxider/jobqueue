FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/job-queue .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/job-queue /job-queue
EXPOSE 2112
ENTRYPOINT ["/job-queue"]
