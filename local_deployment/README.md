# Deploy FAIR stack

DCA is the frontend application to schematic - this means that DCA and schematic can be deployed as one docker compose file.  It may be easier to do local testing in this manner to test configurations.  After filling in all the environmental variables, you can run the following command to start the containers:

```
docker compose up
```

This will start both schematic and DCA. Notice `      DCA_API_HOST: "http://schematic-aws:3001"` in the `docker-compose.yml` file.  This is the host that DCA will use to connect to schematic via the Docker network.
