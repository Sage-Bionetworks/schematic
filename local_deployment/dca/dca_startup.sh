#!/usr/bin/bash

# Pass environment variable to Shiny
echo "" >> .Renviron
echo DCA_SCHEMATIC_API_TYPE=$DCA_SCHEMATIC_API_TYPE >> .Renviron
echo DCA_API_HOST=$DCA_API_HOST >> .Renviron
echo DCA_DCC_CONFIG=$DCA_DCC_CONFIG >> .Renviron
echo DCA_SYNAPSE_PROJECT_API=$DCA_SYNAPSE_PROJECT_API >> .Renviron
echo R_CONFIG_ACTIVE=$R_CONFIG_ACTIVE >> .Renviron
echo DCA_VERSION=$DCA_VERSION >> .Renviron
echo DCA_CLIENT_ID=$DCA_CLIENT_ID >> .Renviron
echo DCA_CLIENT_SECRET=$DCA_CLIENT_SECRET >> .Renviron
echo DCA_APP_URL=$DCA_APP_URL >> .Renviron

# Now run the base start-up script
./startup.sh
