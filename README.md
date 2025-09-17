# README.md

---
## Setup
Add a `.env` file in the root folder with the following content:

### Email Configuration
```env
DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY=["petra.muster@bs.ch", "peter.muster@bs.ch"]
DATASPOT_EMAIL_RECEIVERS=["petra.muster@bs.ch", "peter.muster@bs.ch"]
DATASPOT_EMAIL_SERVER=
DATASPOT_EMAIL_SENDER=
```


### Authentication Configuration

#### M2M Authentication (Recommended)
For machine-to-machine authentication using Azure AD/Entra ID:
```env
# Azure AD/Entra ID configuration
DATASPOT_TENANT_ID=your-azure-tenant-id
DATASPOT_CLIENT_ID=your-entra-app-client-id
DATASPOT_CLIENT_SECRET=your-entra-app-client-secret
DATASPOT_EXPOSED_CLIENT_ID=5d25a...612

# Dataspot service user access key
DATASPOT_SERVICE_USER_ACCESS_KEY=your-service-user-access-key
```

#### Legacy Username/Password Authentication (Deprecated for DataspotAuth)
The following environment variables can be deleted, if they still exist. They are from a legacy system.
```env
DATASPOT_EDITOR_USERNAME
DATASPOT_EDITOR_PASSWORD
DATASPOT_ADMIN_USERNAME
DATASPOT_ADMIN_PASSWORD

DATASPOT_CLIENT_ID
DATASPOT_AUTHENTICATION_TOKEN_URL
DATASPOT_API_BASE_URL
```

> **Note:** The authentication system has been updated to use M2M authentication. The legacy username/password authentication may be deprecated in the future.

---
## Managing (Data Owner) Posts

The following gif shows everything needed to create a new post, and link the correct person. Note that we don't actually need to create the person or user, as this happens daily automatically.
![Setting up a new data owner](docs/data_owner_post.gif)


---
## Integrating code from a `dev` (or `feature`) environment into `prod` [Work-In-Progress]
When integrating a `dev` into `prod`, first we need to clone the `dev` into an `int` database.

Then:
1. Export DNK from `dev` as xlsx and import it again (dry run is enough).
1. If we don't fix warnings or errors that occur, then they will appear later again.
1. Integrate yaml from `dev` into `int`
1. Run job "Regelverletzungen prüfen"
1. Export DNK as xlsx and import it again (dry run is enough)
1. Export and reimport other models that might be affected aswell
1. Merge `dev` into `main` and delete `dev` branch

If everything worked without errors, we can apply the `int` yaml into the `prod` yaml and reapply the changes made to the `int` to the `prod`.

After that, delete the `dev` branch on github, in dataspot, and also its corresponding Annotations.yaml. Also delete the `int` environment in dataspot.


<!-- 
## How to do regular updates (not yet implemented):
Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot. but instead through a file managed by the Data Competence Center DCC. This means that fields that should be updated outside of the workflow are written to the centrally managed file instead of dataspot directly. These changes are then regularly updated by a script from the DCC to dataspot. The key should always be the dataspot-internal UUID. Dates should be provided as Unix timestamps in in UTC timezone. Times should be provided in Unix timestamps aswell in a ??? format (TBD; the same as is used internally in dataspot.). TODO: Add examples
**(put on hold)**

Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot, as this does not work with the workflow. Instead, the changes are pushed to a non-public dataset on [opendatasoft](data.bs.ch). Please [get in touch](mailto:opendata@bs.ch) with us for the setup.

The columns should be (so far): uuid,lastactl,lastpub

This is put on hold for the moment, as lastactl does not really need to be in dataspot. (?)
---
-->
