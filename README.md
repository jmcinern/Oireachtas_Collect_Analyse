# Data Collection

## Overview

**The data is politicians speaking to eachother about matters of concern of the Irish government.**

https://www.oireachtas.ie/en/debates/

**Dáil (House of Representatives)**

- 174 TDs, Lower house, elected by public, most power.

**Seanad (Senate)**

- Upper house.
- Can debate, amend but not stop Dáil legislation.
- The 60 members are chosen by panels of industry specific politicians (43), the Taoiseach (11) and university graduates (6).

**Committee**
- Task groups of TDs and senators focused on specific topics (e.g. healthcare).

**PQs (Parliamentary Questions)**

- Questions submitted and directed to specific people for answers.
- Oral/Written but majority are written.
- Oral PQs overlap with Dáil debates
- Can filter written using URL/API to only select written PQs to avoid Dáil debate duplication.


## Timeline

**Data availibility**

Dáil

- 1919 to Present

Seanad
- 1929 to Present

Committee
- 1924 to present

PQs
- Jan 2025? to present

# XML ->  CSV (AI Generated Description)
This document describes how the Akoma Ntoso XML from the Oireachtas is transformed into the flat CSV `debates_all.csv`, and how to interpret each column—especially the `text` field which holds the raw debate content.

1) XML → CSV Mapping
---------------------

- **Top‐level `<debate type="…">`**  
  Each of the four debate types (Dáil, Seanad, Committee, Questions) becomes one set of rows.

- **Document‐level metadata**  
  Extracted from `<preface>` and `<FRBRWork>`:
  - `doc_id`         ← `/identification/FRBRWork/FRBRthis/@value`
  - `date`           ← `<debate date="…">`
  - `title_ga`, `title_en` ← `<preface>/<block name="title_ga|en">/<docTitle>`
  - `proponent_ga`,`proponent_en` ← `<preface>/<block name="proponent_ga|en">/<docProponent>`
  - `status_ga`, `status_en`   ← `<preface>/<block name="status_ga|en">/<docStatus>`
  - `document_date` ← `<preface>/<block name="date_en">/<docDate>@date`
  - `volume`, `number` ← `<preface>/<docNumber>@refersTo="#vol_…|#no_…"`

- **Per‐element rows**  
  Inside each `<debateBody>`, elements are normalized to rows:
  - `element_type = summary`  
    One row per `<summary>`: prelude notes, suspension, interruptions.
  - `element_type = speech`  
    One row per paragraph (`<p>`) in a `<speech>`: includes speaker id/name/role and timestamp.
  - `element_type = attendance` _(committees only)_  
    One row per committee sitting: the `attendance` column holds a semicolon‐joined list of all `<person>` names from the `<rollCall>`.  
  - `element_type = question` _(questions only)_  
    One row per `<question>`: with separate `question` and `written_answer` columns and a combined `text` field.

2) CSV Columns Overview
------------------------

  • **Common metadata**:  
    `doc_id`, `source_type`, `date`, `title_ga`, `title_en`,  
    `proponent_ga`, `proponent_en`, `status_ga`, `status_en`,  
    `document_date`, `volume`, `number`, `committee_name`,  
    `question_type`, `question_number`.

  • **Structural context**:  
    `section_name`, `section_id`, `heading_text`, `heading_time`.

  • **Element descriptors**:  
    `element_type`, `element_id`.

  • **Speaker or person**:  
    `speaker_id`, `speaker_name`, `speaker_role`, `attendance`.

  • **Timing**:  
    `recorded_time`.

  • **Content**:  
    - `text`           ← Raw text of this row’s element.  
    - `question`       ← The question text (only when `element_type=question`).  
    - `written_answer` ← The written answer text (only when `element_type=question`).  

  • **Topic** _(questions only)_  
    The `to="…”` attribute from `<question>`.

3) Understanding the `text` Field
----------------------------------

The `text` column is your primary access to the debate content:
- **For summaries**: shows the summary line exactly as in the XML.
- **For speeches**: shows the full paragraph as spoken by a Deputy or Minister.
- **For attendance**: repeats the roll‑call header (e.g. “Members present:”).
- **For `question` rows**: concatenates the question and the written answer, so you can search Q&A in one go.

If you need to analyze just the spoken words, filter to `element_type="speech"` and read `text`.  
For committee attendance, filter to `element_type="attendance"` and parse the semicolon‐separated `attendance` list.  
For written questions/answers, filter to `element_type="question"` and use `question` vs. `written_answer` or the combined `text`.

4) Example Queries
-------------------

- **All speeches by a given Deputy**:
  ```sql
  SELECT text
    FROM debates_all
   WHERE element_type = 'speech'
     AND speaker_name = 'Deputy Mary Lou McDonald';
