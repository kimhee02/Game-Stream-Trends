from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.apis import router as api_router

app = FastAPI(docs_url='/api/docs')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/api')
async def root():
    return {"message": "Hello, API Server of Team gureum"}
               
app.include_router(api_router)

