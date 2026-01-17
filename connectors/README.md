# Adding a New Connector

This guide is intended for the Dataspot admin in the DCC. It outlines the steps required to add a new connector to the system.

## 1. Local Setup

1. Copy the "StatA-Test-DB" folder from the DataExch location: `DCC\Dataspot\DatabaseConnector\Configurations`.
2. Adapt the folder name and details in the `application.properties` file within the newly created folder.

## 2. In the 'dataspot' Repository

1. Copy the `stata-test` folder within the `connectors` folder and rename it for the new connector.
2. Adapt the `Dockerfile` if necessary (not required for SQL User authentication).
3. Push the changes to GitHub and wait for the Docker image to be built automatically.
4. Once built, update the package settings at: `https://github.com/orgs/DCC-BS/packages/container/dataspot%2Fconnectors%2F{CREATED-DOCKER-IMAGE-NAME}/settings`.
5. Change the visibility from **Internal** to **public**.
    * **Important:** Failure to set visibility to public is the most common cause of authentication errors in the Airflow script!

## 3. In the 'dags-airflow2' Repository

1. Copy and adapt the `dcc_dataspot_connector_stata_test.py` script.
2. Copy the `dataspot-connector/stata-test/` folder and rename it accordingly.
3. Adapt the `application.yaml` file in the new `dataspot-connector/{new-folder-name}/` folder if necessary:
    * If the target is an **MS SQL database**, the default driver (`mssql-jdbc-13.2.0.jre11.jar`) is correct.
    * If the target is an **Oracle database**, update the driver to `ojdbc17.jar`.

## 4. Optional Steps

1. Note: A Tufin rule may need to be requested via a ticket (details pending).
2. The steps above cover SQL User authentication. Details for AD-User authentication will follow.