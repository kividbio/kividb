FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum main.go ./
RUN CGO_ENABLED=0 go build -o /fakes3 .
FROM gcr.io/distroless/static
COPY --from=build /fakes3 /fakes3
ENTRYPOINT ["/fakes3"]
