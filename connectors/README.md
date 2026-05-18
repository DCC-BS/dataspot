## Datenbank-Anbindung an den Kantonalen Datenkatalog

Für die Anbindung einer Datenbank an den Kantonalen Datenkatalog arbeitet die Dienststelle mit dem DCC zusammen und stellt die unten aufgeführten technischen und organisatorischen Angaben bereit.

### Ablauf einer Datenbank-Anbindung

1. Dienststelle erstellt den SQL-User oder Kerberos-User (AD-User) und liefert die technischen Basisangaben.
   - Details: [Was die Dienststelle angeben muss](#was-die-dienststelle-angeben-muss)
2. DCC bindet die Datenbank technisch an.
   - Details: [README_DCC.md](./README_DCC.md)
3. DCC importiert die Daten in die Test-Umgebung.
   - Details: [Checkliste für DCC](#checkliste-für-dcc)
4. Dienststelle reviewed die Daten und entscheidet über Allowlist/Denylist (Schemas, Tabellen, Views).
   - Details: [Was die Dienststelle angeben muss](#was-die-dienststelle-angeben-muss)
5. DCC und Dienststelle klären Betriebsparameter und Data Governance.
   - Aktualisierungsfrequenz
   - Systembeschreibung
   - Data Governance: Data Owner, Data Steward, Data Custodian
   - Details: [Checkliste für DCC](#checkliste-für-dcc)
6. DCC importiert die Daten in die Prod-Umgebung. Damit ist die Anbindung der Datenbank abgeschlossen; anschliessend können fachliche Daten erfasst werden (z. B. Datenprodukte, Fachdaten).

### Was die Dienststelle angeben muss

1. Datenbank-Typ und Version
   - Typ (z. B. `MS SQL`, `Oracle`, `PostgreSQL`)
   - Version (z. B. `SQL Server 2019`, `Oracle 19c`)
   - Beispiel:
     - `db_typ: Oracle`
     - `db_version: 19c`

2. Datenbank-Instanz / Erreichbarkeit
   - Mindestens die Verbindungs-URL bzw. der Connection String (siehe DB-spezifische Abschnitte)
   - Optional zusätzlich Host, Port, Datenbankname/SID/Service-Name
   - Beispiel (Oracle):
     - `database_url: jdbc:oracle:thin:CUSTOM_DB_USER/CUSTOM_DB_USER_PASSWORD@db-test.example.local:1521/db-test`
   - Beispiel (MS SQL):
     - `connection_string: jdbc:sqlserver://test_server;databaseName=test_db;user=test_user;password=test_user_password`

3. Authentifizierungsart

   Die Dienststelle erstellt den technischen Zugang (SQL-User oder Kerberos-User) und übergibt die Zugangsdaten an das DCC.

   - SQL-User (technischer User)
     - benötigt: Benutzername und Passwort
     - Beispiel:
       - `username: TESTMETADATEN`
       - `password: ********`
   - Kerberos-User (AD-User, nicht Azure)
     - benötigt: Benutzername und Passwort (wie beim SQL-User)
     - Beispiel:
       - `username: test_metadaten`
       - `password: ********`

4. Nutzer-Berechtigung
   - Siehe den zentralen Abschnitt [Nutzer-Berechtigung](#nutzer-berechtigung-zentrales-thema).

### Nutzer-Berechtigung

Dieser Abschnitt ist zentral für die Anbindung. Die Berechtigungen sollten, wenn möglich und nötig, so vergeben werden, dass der Connector nur die freigegebenen Informationen sehen kann.

#### MS SQL

**Kurze Beschreibung**

- Mit `VIEW DEFINITION` werden Metadaten sichtbar (z. B. Tabellen, Spalten, Datentypen).
- Diese Berechtigungen geben keine pauschale Berechtigung auf Tabelleninhalte (direkte `SELECT`-Abfragen auf Daten bleiben ohne zusätzliche Rechte weiterhin eingeschränkt).
- Die Wahl der Berechtigungsebene (Datenbank, Schema, Tabelle) steuert, wie breit der Zugriff auf Metadaten ist.

**Commands**

Option 1: Schema-Ebene

```sql
GRANT VIEW DEFINITION ON SCHEMA::<schema_name> TO <username>;
```

Beispiel:

```sql
GRANT VIEW DEFINITION ON SCHEMA::astro TO test_user;
GRANT VIEW DEFINITION ON SCHEMA::cats TO test_user;
GRANT VIEW DEFINITION ON SCHEMA::dogs TO test_user;
```

Option 2: Datenbank-Ebene

```sql
GRANT VIEW DEFINITION ON DATABASE::<database_name> TO <username>;
```

Option 3: Tabellen-Ebene

```sql
GRANT VIEW DEFINITION ON OBJECT::<schema>.<table> TO <username>;
```

#### Oracle

**Kurze Beschreibung**

- Im dokumentierten KDM-Fall wurde `SELECT_CATALOG_ROLE` auf alle relevanten Schemas verwendet.
- Zusätzlich gab es einen Metadaten-Sonderfall (`all_*` vs. `dba_*`), der über Synonyme gelöst wurde.
- Konkrete zusätzliche Oracle-GRANT-Commands sind in den vorhandenen Quellen nicht weiter dokumentiert und werden hier nicht ergänzt.

**Commands (dokumentiert)**

```sql
create or replace synonym CUSTOM_DB_USER.all_tab_columns for dba_tab_columns;
create or replace synonym CUSTOM_DB_USER.all_objects for dba_objects;
create or replace synonym CUSTOM_DB_USER.all_tables for dba_tables;
create or replace synonym CUSTOM_DB_USER.all_tab_cols for dba_tab_cols;
create or replace synonym CUSTOM_DB_USER.all_users for dba_users;
create or replace synonym CUSTOM_DB_USER.all_constraints for dba_constraints;
create or replace synonym CUSTOM_DB_USER.all_cons_columns for dba_cons_columns;
create or replace synonym CUSTOM_DB_USER.all_tab_comments  for dba_tab_comments;
create or replace synonym CUSTOM_DB_USER.all_types for dba_types;
create or replace synonym CUSTOM_DB_USER.all_col_comments for dba_col_comments;
```

### Sicherheits- und Betriebs-Hinweis zum Connector-JAR

- Das DCC erhält vom Dataspot ein Connector-JAR und führt dieses aus.
- Das DCC führt kein Audit des JAR-Inhalts durch.
- Eine Supply-Chain-Attacke ist grundsätzlich möglich.
- Das JAR ist daher so zu behandeln, als könnte es im schlimmsten Fall auch Tabelleninhalte auslesen, sofern die Berechtigungen dies erlauben.
- Die Nutzer-Berechtigungen müssen entsprechend restriktiv und zielgerichtet vergeben werden.
- Je nach Datenbank-Art kann der Connector ungeeignet sein, wenn Tabelleninhalte unter keinen Umständen verfügbar sein dürfen.

### MS SQL

Anforderungen für die Anbindung einer MS SQL-Datenbank an den Datenkatalog.

#### Benötigte Informationen

**Connection String**

JDBC Connection String mit folgender Struktur:

```
jdbc:sqlserver://<servername>;databaseName=<database>;user=<username>;password=<password>
```

Beispiel:

```
jdbc:sqlserver://test_server;databaseName=test_db;user=test_user;password=test_user_password
```

Bei AD-Authentifizierung (Kerberos) werden Benutzername und Passwort nicht im Connection String hinterlegt; siehe [README_DCC.md](./README_DCC.md).

#### Bekannte Erfahrung (Kerberos/AD)

- In einem Fall hatte der Kerberos-User (BS-AD) keine ausreichenden Berechtigungen mehr.
- Lösung in diesem Fall: SQL-Login löschen und neu anlegen.
- Das folgende Statement war laut Notiz anschliessend **nicht mehr nötig**:
  - `create login [BS\\<AD_BENUTZERNAME>] from windows with default_database=[master];`

### Bekannte Möglichkeiten und Limitationen

- Auth-Varianten (empfohlen: SQL-User):
  - SQL-User (Username/Passwort)
  - AD/Kerberos (Benutzername/Passwort)
- Bekannte Oracle-Limitation (bereits aufgetreten):
  - Der Connector fragt Metadaten über `all_*` Views ab.
  - In einem konkreten Oracle-Setup waren aber `dba_*` Views relevant.
  - Ohne Workaround wurden Tabellen/Spalten nicht korrekt gefunden.

### Oracle-Sonderfall (dokumentierter Workaround)

Falls bei Oracle Metadaten nicht sichtbar sind, wurde folgender Workaround erfolgreich eingesetzt: Synonyme im Benutzer-Schema auf `dba_*` Views setzen.

Beispiele (dokumentiert):

Siehe die Commands im Abschnitt [Nutzer-Berechtigung](#nutzer-berechtigung-zentrales-thema).

### Checkliste für DCC

- Data Owner erfasst
- Data Steward erfasst
- Data Custodian erfasst
- System erfasst
- Aktualisierungsfrequenz erfasst
- Systembeschreibung erfasst
