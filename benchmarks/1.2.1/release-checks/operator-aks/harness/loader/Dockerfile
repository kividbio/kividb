FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /loader .
FROM gcr.io/distroless/static
COPY --from=build /loader /loader
ENTRYPOINT ["/loader"]
