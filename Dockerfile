FROM golang:1.23-alpine

# Install required tools
RUN apk add --no-cache \
    git \
    make \
    curl

# Set working directory
WORKDIR /app

# Copy go mod files first for caching
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download || true

# Copy source code
COPY . .

# Expose gRPC port
EXPOSE 50051

# Default command
CMD ["go", "run", "."]
