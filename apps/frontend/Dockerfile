FROM node:20-alpine

WORKDIR /app

# Gebruik CommonJS, dus geen "type": "module" nodig
# Als je al een package.json hebt, gebruik die i.p.v. init:
RUN npm init -y && npm i express

COPY server.js ./server.js
COPY public ./public

ENV PORT=6000
EXPOSE 6000
CMD ["node", "server.js"]
