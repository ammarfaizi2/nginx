# Stage 1: Build environment
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    wget \
    lsb-release \
    software-properties-common \
    gnupg \
    make \
    ca-certificates \
    libxml2-dev \
    libxslt1-dev \
    libgd-dev \
    libgeoip-dev \
    libpcre3-dev \
    libssl-dev \
    zlib1g-dev \
    gcc

# Copy the Nginx source code into the container
WORKDIR /usr/src/nginx
COPY . .

# Configure, compile, and install Nginx
RUN CC=gcc ./auto/configure \
    --prefix=/usr/share/nginx \
    --sbin-path=/usr/sbin/nginx \
    --modules-path=/usr/lib/nginx/modules \
    --conf-path=/etc/nginx/nginx.conf \
    --error-log-path=/var/log/nginx/error.log \
    --http-log-path=/var/log/nginx/access.log \
    --pid-path=/var/run/nginx.pid \
    --lock-path=/var/run/nginx.lock \
    --http-client-body-temp-path=/var/cache/nginx/client_temp \
    --http-proxy-temp-path=/var/cache/nginx/proxy_temp \
    --http-fastcgi-temp-path=/var/cache/nginx/fastcgi_temp \
    --http-uwsgi-temp-path=/var/cache/nginx/uwsgi_temp \
    --http-scgi-temp-path=/var/cache/nginx/scgi_temp \
    --user=nginx \
    --group=nginx \
    --with-threads \
    --with-file-aio \
    --with-http_ssl_module \
    --with-http_v2_module \
    --with-http_v3_module \
    --with-http_realip_module \
    --with-http_addition_module \
    --with-http_xslt_module \
    --with-http_image_filter_module \
    --with-http_geoip_module \
    --with-http_sub_module \
    --with-http_dav_module \
    --with-http_flv_module \
    --with-http_mp4_module \
    --with-http_gunzip_module \
    --with-http_gzip_static_module \
    --with-http_auth_request_module \
    --with-http_random_index_module \
    --with-http_secure_link_module \
    --with-http_degradation_module \
    --with-http_slice_module \
    --with-http_stub_status_module \
    --with-mail \
    --with-stream \
    --with-stream_ssl_module \
    --with-stream_realip_module \
    --with-stream_geoip_module \
    --with-stream_ssl_preread_module \
    --with-pcre \
    --with-pcre-jit \
    --add-module=./addons/or_limit \
    && make -j$(nproc) \
    && make install

# Stage 2: Minimal runtime environment
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install ONLY the runtime libraries required by Nginx (no -dev packages or build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxslt1.1 \
    libgd3 \
    libgeoip1 \
    libpcre3 \
    openssl \
    zlib1g \
    ca-certificates \
    cron \
    logrotate \
    && rm -rf /var/lib/apt/lists/*

# Move logrotate to cron.hourly so it checks sizes more frequently
RUN mv /etc/cron.daily/logrotate /etc/cron.hourly/logrotate

# Create nginx user/group and required directories
RUN addgroup --system nginx \
    && adduser --system --disabled-login --ingroup nginx --no-create-home --home /nonexistent --gecos "nginx user" --shell /bin/false nginx \
    && mkdir -p /var/cache/nginx/client_temp \
    && mkdir -p /var/cache/nginx/proxy_temp \
    && mkdir -p /var/cache/nginx/fastcgi_temp \
    && mkdir -p /var/cache/nginx/uwsgi_temp \
    && mkdir -p /var/cache/nginx/scgi_temp \
    && mkdir -p /var/log/nginx \
    && chown -R nginx:nginx /var/cache/nginx /var/log/nginx

# Copy the compiled Nginx installation from the builder stage
COPY --from=builder /usr/sbin/nginx /usr/sbin/nginx
COPY --from=builder /etc/nginx /etc/nginx
COPY --from=builder /usr/share/nginx /usr/share/nginx

# Copy logrotate config and entrypoint
COPY logrotate.conf /etc/logrotate.d/nginx
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run Nginx and cron
CMD ["/entrypoint.sh"]
