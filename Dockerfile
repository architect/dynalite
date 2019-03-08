FROM node:lts-alpine

ENTRYPOINT ["/usr/local/bin/dynalite"]
EXPOSE 4567
RUN adduser -h /var/lib/dynamodb -D -u 4567 dynalite

COPY . /usr/local/share/dynalite

RUN apk add --no-cache --virtual .gyp \
        python \
        make \
        g++ \
    && npm --unsafe-perm install \
        -g /usr/local/share/dynalite \
    && npm --force cache clear \
    && apk del .gyp \
    && rm -rf /tmp/* /var/tmp/* /var/cache/*

USER dynalite
