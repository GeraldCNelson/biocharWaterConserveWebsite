# App Structure Overview

This markdown document outlines the high-level structure of the Biochar Water Conservation web application,
including the frontend and backend components.

## 📁 Project Root

```
biochar_app/
│
├── static/
│   ├── css/
│   ├── js/
│   │   ├── main.js
│   │   ├── control_panel.js
│   │   ├── plots.js
│   │   ├── tables.js
│   │   ├── plot_utils.js
│   │   ├── ui_controls.js
│   │   ├── ui_utils.js
│   │   └── config.js
│   └── ...
│
├── templates/
│   └── index.html
│
├── markdown/
│   ├── intro.md
│   ├── experimentDesign.md
│   ├── techDetails.md
│   ├── help_main.md
│   └── help_summary.md
│
├── data/
│   ├── processed/
│   └── raw/
│
├── app.py
├── config.py
├── routes.py
├── routes_utils.py
├── utils.py
├── plot_utils.py
└── ...
```

## 🧠 Key Components

- `app.py`: Entry point that initializes the Flask application.
- `routes.py`: Defines backend API endpoints (data, plot, summary stats).
- `utils.py`: Shared helper functions (e.g., loading irrigation events).
- `plot_utils.py`: Plot formatting and serialization logic.
- `routes_utils.py`: Functions used across multiple route handlers.
- `config.py`: Centralized configuration constants for the backend.
- `templates/index.html`: Main HTML file loaded in the browser.
- `static/js/`: JavaScript modules for frontend interaction and rendering.
- `markdown/`: Markdown content for dynamic page sections.

## 🔄 Data Flow Summary

1. **Initialization**: `main.js` loads dropdown options and default states.
2. **Plot updates**: Triggered by `Update Plots` button → `plot_data()` → `prepare_plot_figure()`.
3. **Summary stats**: Triggered by `Update Summary` button → `get_summary_stats()` → table rendering.
4. **Markdown content**: Loaded dynamically via `loadMarkdownContent()`.

## ✅ Maintainer Tips

- Use `control_panel.js` for dropdown/interaction logic across tabs.
- Keep `config.js` and `config.py` in sync for default values and labels.
- Use `developer_notes.md` for hard-earned implementation knowledge.