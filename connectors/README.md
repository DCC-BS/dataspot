# Adding a New Connector

This guide is intended for the Dataspot admin in the DCC. It outlines the steps required to add a new connector to the system.

Connectors can authenticate to databases using one of two methods:
- **SQL User authentication**: Uses a SQL username and password stored in the configuration files.
- **AD User authentication**: Uses Active Directory (Kerberos) authentication with credentials managed via Airflow Variables.

Choose the appropriate section below based on your authentication method.

---

## SQL User Authentication

Use this method when connecting with a SQL database user. This is based on the `stata-test` template.

### 1. Local Setup

1. Copy the `StatA-Test-DB` folder from the DataExch location: `DCC\Dataspot\DatabaseConnector\Configurations`.
2. Rename the folder and adapt the details in the `application.properties` file within the newly created folder, including username and password.

### 2. In the 'dataspot' Repository

1. Copy the `stata-test` folder within the `connectors` folder and rename it for the new connector.
2. Adapt the `Dockerfile` if necessary (usually not required for SQL User authentication).
3. Push the changes to GitHub and wait for the Docker image to be built automatically.

### 3. In the 'dags-airflow2' Repository

1. Copy and adapt the `dcc_dataspot_connector_stata_test.py` script.
2. Copy the `dataspot-connector/stata-test/` folder and rename it accordingly.
3. Adapt the `application.yaml` file in the new `dataspot-connector/{new-folder-name}/` folder if necessary.

---

## AD User Authentication (Kerberos)

Use this method when connecting with an Active Directory user via Kerberos. This is based on the `stata-ad-test` template.

### 1. Local Setup

1. Copy the `StatA-Test-DB_AD-Access` folder from the DataExch location: `DCC\Dataspot\DatabaseConnector\Configurations`.
2. Rename the folder and adapt the `application.properties` file within the newly created folder.
   - **Important**: Only update `server` and `database_name`. Do NOT set username and password here; these are managed via Airflow Variables.

### 2. In the 'dataspot' Repository

1. Copy the `stata-ad-test` folder within the `connectors` folder and rename it for the new connector.
2. The `Dockerfile` and `entrypoint.sh` already contain the Kerberos/AD setup. They use 4 environment variables which are passed from the Airflow script, 2 of which need to be added in airflow:
   - `XYZ_AD_USERNAME`
   - `XYZ_AD_PASSWORD`

3. Push the changes to GitHub and wait for the Docker image to be built automatically.

### 3. In the 'dags-airflow2' Repository

1. Copy and adapt the `dcc_dataspot_connector_stata_ad_test.py` script.
2. Ensure the image path uses the `connectors/{new-folder}` format.
3. **Critical**: Ensure the following Airflow Variables are configured:
   - `XYZ_AD_USERNAME` - The AD username (without domain prefix)
   - `XYZ_AD_PASSWORD` - The AD password
4. Copy the `dataspot-connector/stata-ad-test/` folder and rename it accordingly.
5. Adapt the `application.yaml` file in the new `dataspot-connector/{new-folder-name}/` folder if necessary (see [Driver Selection](#driver-selection)).

---

## Common Notes (Both Methods)

### Package Visibility

After pushing to GitHub and the Docker image is built:
1. Update the package settings at: `https://github.com/orgs/DCC-BS/packages/container/dataspot%2Fconnectors%2F{CREATED-DOCKER-IMAGE-NAME}/settings`.
2. Change the visibility from **Internal** to **public**.
   - **Important:** Failure to set visibility to public is the most common cause of authentication errors in the Airflow script!

### Driver Selection

In the `application.yaml` file:
- If the target is an **MS SQL database**, the default driver (`mssql-jdbc-13.2.0.jre11.jar`) is correct.
- If the target is an **Oracle database**, update the driver to `ojdbc17.jar`.

### Optional Steps

1. A Tufin rule may need to be requested via a ticket (details pending).
