image_config = {
    "intro.docx": {
        "output_md": "intro.md",
        "images": [
            {
                "file": "biocharMicro1.jpg",
                "caption": "Figure 1. Scanning electron microscope image of biochar",
                "alt": "Scanning electron microscope image of biochar"
            },
            {
                "file": "lignin_diagram.jpg",
                "caption": "Figure 2. Lignin chemical structure",
                "alt": "Diagram showing lignin structure"
            },
            {
                "file": "biochar_diagram.jpg",
                "caption": "Figure 3. Biochar chemical structure",
                "alt": "Diagram showing biochar chemical structure"
            }
        ],
        "side_by_side": [
            ["lignin_diagram.jpg", "biochar_diagram.jpg"]
        ]
    },
    "experimentDesign.docx": {
        "output_md": "experimentDesign.md",
        "images": [
            {
                "file": "biocharExperimentalDesign.jpg",
                "caption": "Field experimental layout",
                "alt": "Layout of biochar plots"
            },
            {
                "file": "biochar_closeup.jpg",
                "caption": "Closeup image of biochar material",
                "alt": "Closeup of biochar material"
            }
        ]
    },
    "techDetails.docx": {
        "output_md": "techDetails.md"
    }
}

modal_config = {
    "main": {
        "source": "main_data_display_modal.docx",
        "output": "main_data_display_modal.md"
    },
    "summary": {
        "source": "summary_statistics_modal.docx",
        "output": "summary_statistics_modal.md"
    }
}