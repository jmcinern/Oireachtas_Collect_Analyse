debates_all.csv
================

This CSV contains a unified export of all parliamentary debates, questions, and committee sittings
from the Oireachtas Akoma Ntoso XML. Each row represents one “element” of the debate body: a summary,
a speech, a written question/answer, or an attendance record.

Columns:

  • doc_id             – Unique document identifier (FRBRthis value)
  • source_type        – Debate type: “dail”, “seanad”, “committee”, or “questions”
  • date               – Date attribute of the debate (YYYY‑MM‑DD)
  • title_ga           – Irish language document title
  • title_en           – English language document title
  • proponent_ga       – Irish language proponent name (e.g. DÁIL ÉIREANN)
  • proponent_en       – English language proponent name
  • status_ga          – Irish language document status (e.g. “TUAIRISC OIFIGIÚIL”)
  • status_en          – English language document status (e.g. “(OFFICIAL REPORT)”)
  • document_date      – Date from <docDate> block (ISO date)
  • volume             – Volume number (e.g. “Vol. 1062”)
  • number             – Issue number (e.g. “No. 2”)
  • committee_name     – For committee sittings, the slug/name of the committee
  • question_type      – For “questions” rows, the question type attribute
  • question_number    – For “questions” rows, the question number attribute

  • section_name       – Name attribute of the debateSection (e.g. “prelude”, “debate”)
  • section_id         – eId attribute of the debateSection
  • element_type       – Type of element in the body:
                         “summary”, “speech”, “attendance”, or “question”
  • element_id         – eId attribute of the element (summary/speech/question)
  • speaker_id         – eId reference for the speaker (if applicable)
  • speaker_name       – Name of the speaker or person in attendance
  • speaker_role       – Role attribute (e.g. ministerial office or chair)
  • recorded_time      – Timestamp when the element was recorded
  • topic              – For questions: the @to attribute (the topic)
  • question           – The question text (for “question” rows)
  • written_answer     – The answer text (for “question” rows)
  • text               – The raw concatenated text of the element (summary, speech, or Q+A)
  • heading_text       – If inside a heading, the heading text
  • heading_time       – Timestamp on the <heading> element
  • attendance         – Semicolon‑separated list of names present (for “attendance” rows)

Notes:
- Rows where element_type=attendance appear once per debate (committee sittings),
  with all names joined into a single attendance cell.
- Sections and speeches produce multiple rows per debate.
- University of Dublin timezone: all timestamps are in +00:00 or +01:00 depending on DST.