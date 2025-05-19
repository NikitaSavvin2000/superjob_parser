import uvicorn
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

from src.config import  public_or_local

if public_or_local == 'LOCAL':
    url = 'http://localhost'
else:
    url = 'http://11.11.11.11'

origins = [
    url
]

app = FastAPI(docs_url="/template_fast_api/v1/", openapi_url='/template_fast_api/v1/openapi.json')
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "Welcome to the indicators System API"}


if __name__ == "__main__":
    port = 7070
    uvicorn.run(app, host="0.0.0.0", port=port)
