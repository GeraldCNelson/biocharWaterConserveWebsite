# Recovery Notes — PyCharm Upgrade Incident

**Date:** 2026-02  
**Context:** Biochar Water Conservation Website (FastAPI + JS frontend)

## What Happened

During work on the Biochar project, a temporary chat session was used to debug and fix multiple issues following a PyCharm upgrade.  
That temporary chat produced a useful recovery document, but it was **not successfully saved or downloaded** and is now lost.

As a result, the exact recovery steps are no longer available.

This file records the *key recovery lessons* that must not be forgotten.

---

## Critical Lessons for Future Jerry

### a. You have a Time Machine backup

If files, notes, or recovery documents are lost again:

- **Stop immediately**
- Use **macOS Time Machine** to restore:
  - the project directory
  - notes
  - temporary working files
  - drafts that were not yet committed

Do **not** assume that:
- chat history
- temporary buffers
- unsaved editor tabs
- clipboard contents

will still be available later.

They often are not.

---

### b. Hidden files *are* recoverable (and easy to miss)

Files and directories starting with a dot (`.`) are hidden by default on macOS.

These **can and do matter**, especially for:
- virtual environments
- configuration
- editor state
- caches
- `.env`, `.venv`, `.git`, etc.

#### How to reveal hidden files

**In Finder (local filesystem):**
- Press **Shift + Command + .**  
  (toggles hidden files on/off)

**In Time Machine:**
- The *same shortcut works*
- Hidden files are recoverable once visible

If something “mysteriously disappeared,” check whether it was hidden.

---

### c. ⬜ (intentionally left blank)

There was at least one additional recovery insight discovered during this incident,
but it could not be reconstructed reliably after the temporary chat was lost.

If you remember what this was later:
- add it here
- commit immediately

---

## What To Do Next Time (Checklist)

1. **If something important is written but not committed**
   - Save it as a local `.md` file immediately
   - Commit it, even if marked `DRAFT`

2. **If files disappear or content is lost**
   - Pause further changes
   - Open Time Machine
   - Restore the project directory from *before* the loss

3. **After recovery**
   - Commit recovered files with a clear message:
     ```
     docs: recover lost notes after PyCharm upgrade
     ```

---

## Notes

- This document is intentionally minimal and factual.
- It records only what is known to be true.
- The original recovery steps cannot be reconstructed reliably and are therefore not included.

If you are reading this in the future:  
**Time Machine + hidden files visibility will save you hours.**