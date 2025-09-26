FROM node:20-alpine
WORKDIR /app

# CommonJS, geen ESM gedoe
RUN npm init -y && npm i express

# Deze paden werken *alleen* als je build context = apps/frontend
COPY server.js ./server.js
COPY public ./public

ENV PORT=6000
EXPOSE 6000
CMD ["node", "server.js"]
