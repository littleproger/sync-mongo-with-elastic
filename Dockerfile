# Use the specific version of the Node.js image
FROM node:18.17.0

# Create app directory in the Docker image
WORKDIR /usr/src/app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install app dependencies including 'devDependencies'
# Note: Consider using --only=production for production builds
RUN npm install

# Bundle app source inside Docker image
COPY . .

# Copy your certificate file into the Docker image
# Make sure the certificate file is in the same directory as your Dockerfile
COPY ./http_ca.crt ./

# Build the TypeScript files
RUN npm run build

# Your application's default environment variables
# Set them here if they have default values
ENV MONGODB_URL_WITH_REPLICA_SET=yourMongoDBUrl
ENV ELASTIC_URL=yourElasticURL
ENV INDEX_PREFIX=yourPrefix
ENV IS_DEBUG=false
ENV WITH_INITIAL_SYNC=false
ENV PATH_TO_CERTIFICATE=./http_ca.crt
ENV IS_CERTIFICATE_SELF_SIGNED=false

# Start the application
CMD [ "node", "dist/lib/docker.js" ]
