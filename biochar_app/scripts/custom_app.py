# biochar_app/scripts/custom_app.py

import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from biochar_app.scripts.routes_custom_gseason import router as custom_gseason_router

app = FastAPI()

# include your custom-gseason router
app.include_router(custom_gseason_router)

# 1) root → redirect
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/custom-gseason")


# 2) custom startup log
@app.on_event("startup")
async def announce():
    print("👉  Your custom growing-season page is at http://127.0.0.1:8001/custom-gseason\n")


if __name__ == "__main__":
    uvicorn.run(
        "biochar_app.scripts.custom_app:app",  # must be module:name
        host="127.0.0.1",
        port=8001,
        reload=True,
    )