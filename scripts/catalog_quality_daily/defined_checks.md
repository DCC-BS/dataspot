Quelle: https://datenkatalog.bs.ch/web/prod/collections/9c0b67a5-9cc8-4bcc-a8d8-e9f03918a0a6

---

Abhängigkeiten und Zweck:

1. Eindeutigkeitsprüfung  
   - Abhängigkeiten: Keine (Grundlagenprüfung - muss zuerst laufen)  
   - Zweck: Stellt Datenintegrität sicher, bevor Personen erstellt/aktualisiert werden

2. Personensynchronisation aus dem Staatskalender  
   - Abhängigkeiten: Check #1 (Eindeutigkeitsprüfung)  
   - Zweck: Erstellt/aktualisiert Personen aus Staatskalender nach Sicherstellung, dass keine Duplikate existieren

3. Mitgliedschaftsbasierte Posten-Zuordnungen  
   - Abhängigkeiten: Check #2 (Personensynchronisation), Check #1 (Eindeutigkeitsprüfung)
   - Zweck: Weist Personen Posten basierend auf Staatskalender-Mitgliedschaftsdaten zu, nachdem Personen existieren

4. Postenbesetzungsprüfung  
   - Abhängigkeiten: Check #3 (Mitgliedschaftsbasierte Posten-Zuordnungen)  
   - Zweck: Überprüft, dass alle Posten besetzt sind, nachdem Zuweisungen vorgenommen wurden

5. Benutzerkontensynchronisation  
   - Abhängigkeiten: Check #3 (Mitgliedschaftsbasierte Posten-Zuordnungen), Check #4 (Postenbesetzungsprüfung)  
   - Zweck: Erstellt Benutzerkonten für alle Personen mit sk_person_id (EDITOR bei Posten, sonst READ_ONLY) und weist Berechtigungen zu

6. Kontaktdetails bei Personen
   - Abhängigkeiten: Check #2 (Personensynchronisation aus dem Staatskalender)
   - Zweck: Erstellt/aktualisiert Kontaktdetails aus dem Staatskalender

7. Annotation YAML vs Repo
   - Abhängigkeiten: Keine (unabhängiger Check)
   - Zweck: Stellt sicher, dass die Annotation-YAML-Dateien auf der Webseite mit den Repo-Versionen übereinstimmen

---

1 Eindeutigkeitsprüfung

Alle Personen haben eindeutige sk_person_id Werte.

Spezifisch:
- Für alle Personen mit gesetzter sk_person_id wird geprüft:
	- Die sk_person_id ist eindeutig (keine Duplikate)

Falls nicht:
- Eine E-Mail mit allen Problemen wird an dcc@bs.ch gesendet

---
2 Personensynchronisation aus dem Staatskalender

Alle Personen aus dem Staatskalender sind korrekt in Dataspot vorhanden.

Spezifisch:
- Für alle Posten mit einer Mitgliedschaft-ID wird geprüft:
	- Die Mitgliedschaft-ID existiert im Staatskalender
	- Die im Staatskalender verknüpfte Person ist mit korrektem Namen in Dataspot vorhanden
	- Die Person hat die korrekte sk_person_id gesetzt
- Es werden sowohl primäre als auch sekundäre Mitgliedschaft-ID berücksichtigt

Falls nicht:
- Wenn die Mitgliedschaft-ID ungültig ist, wird dies gemeldet, ohne Änderungen vorzunehmen
- Wenn die Person in Dataspot nicht existiert, wird sie automatisch mit den Daten (Name, sk_person_id) aus dem Staatskalender erstellt
- Wenn die Person existiert, aber falsche Daten hat, werden diese automatisch aktualisiert (Name, sk_person_id)
- Eine E-Mail mit allen Problemen und Änderungen wird an dcc@bs.ch gesendet

---
3 Mitgliedschaftsbasierte Posten-Zuordnungen

Alle Posten mit Mitgliedschaft-IDs haben korrekte Personen-Zuordnungen basierend auf den Staatskalender-Daten.

Spezifisch:
- Für alle Posten mit Mitgliedschaft-ID wird geprüft:
	- Die Person aus dem Staatskalender ist korrekt dem Posten zugeordnet
	- Nur Personen mit gültigen Mitgliedschaft-IDs sind dem Posten zugeordnet
- Es werden sowohl primäre als auch sekundäre Mitgliedschaft-ID berücksichtigt

Falls nicht:
- Wenn die Person nicht dem Posten zugeordnet ist, wird die Zuordnung automatisch hergestellt
- Wenn andere Personen dem Posten zugeordnet sind, werden diese entfernt (nur für Posten mit Mitgliedschaft-IDs)
- Eine E-Mail mit allen Problemen und Änderungen wird an dcc@bs.ch gesendet

---
4 Postenbesetzungsprüfung

Alle Posten sind von mindestens einer Person besetzt.

Spezifisch:
- Für alle Posten wird geprüft:
	- Mindestens eine Person ist dem Posten zugeordnet

Falls nicht:
- Eine E-Mail mit allen Problemen wird an dcc@bs.ch gesendet

---
5 Benutzerkontensynchronisation

Alle Personen mit sk_person_id haben korrekte Benutzerkonten.

Spezifisch wird für alle Personen mit gesetzter sk_person_id überprüft:
- Ein Benutzer mit der korrekten E-Mail-Adresse aus dem Staatskalender existiert
- Der Benutzer ist über das isPerson-Feld korrekt mit der Person verknüpft
- Wenn die Person einen Posten hat, hat der Benutzer mindestens EDITOR Zugriffsrechte

Falls nicht:
- Wenn keine E-Mail-Adresse im Staatskalender hinterlegt ist, wird dies gemeldet
- Wenn kein Benutzer für die Person existiert, wird ein Benutzer erstellt:
	- Mit der E-Mail-Adresse aus dem Staatskalender
	- Mit EDITOR Zugriffsrechten falls die Person einen Posten hat, sonst READ_ONLY
- Wenn der Benutzer nicht korrekt mit der Person verknüpft ist, wird der Benutzer mit der korrekten Person verknüpft
- Wenn die Zugriffsrechte des Benutzers READ_ONLY sind und die Person einen Posten hat, werden sie zu EDITOR geändert
- Benutzer mit ADMINISTRATOR Zugriffsrechten werden nie verändert
- Eine E-Mail mit allen Problemen und Änderungen wird an dcc@bs.ch gesendet

---
6 Kontaktdetails bei Personen  

Alle Personen mit sk_person_id haben die korrekten Kontakt-Daten (Tel/E-Mail/Teams-Link/Kontaktwebseite) aus dem Staatskalender.

Spezifisch wird für alle Personen mit gesetzter sk_person_id überprüft:  
- Die Telefonnummer im Staatskalender und in dataspot sind identisch
- Die E-Mail Adresse im Staatskalender und in dataspot sind identisch
- Der Teams-Link enthält die korrekte E-Mail Adresse
- Die Kontaktwebseite ist korrekt ausgefüllt

Falls nicht:
- Wenn die Details nicht übereinstimmen werden die Details in dataspot angepasst
- Eine E-Mail mit allen Änderungen wird an dcc@bs.ch gesendet

---
7 Annotation YAML vs Repo

Die auf dem Datenkatalog ausgelieferten Annotation-YAML-Dateien entsprechen den im Repo gepflegten Versionen. Es wird immer die Prod-Version verglichen.

Spezifisch:
- Für jede konfigurierte Datei wird geprüft:
	- Die Online-Version wird immer aus Prod bezogen (Download-URL)
	- Die Online-Version wird mit der lokalen Datei (Repo) per Rohtext-Vergleich verglichen
	- Zeilenenden werden normalisiert, um falsche Unterschiede zu vermeiden

Falls nicht:
- Eine E-Mail mit den betroffenen Dateien (welche Datei abweicht) wird an dcc@bs.ch gesendet
- Es werden keine automatischen Änderungen vorgenommen