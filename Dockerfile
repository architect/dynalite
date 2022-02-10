FROM node:alpine

RUN npm install --no-optional -g dynalite \
&& npm cache clear \
&& rm -rf /tmp/* /var/tmp/*

EXPOSE 4567
CMD dynalite
