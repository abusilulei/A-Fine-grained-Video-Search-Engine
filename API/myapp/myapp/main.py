from fastapi import FastAPI, HTTPException, Query
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print("BASE_DIR:", BASE_DIR)
df_raw = pd.read_pickle(os.path.join(BASE_DIR, "data_cleaned_5.pkl"))

from .pkl_api_v6 import SearchData

app = FastAPI()

# CORS middleware
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust as needed for your deployment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/search")
async def search_keyword(
    keyword: str = Query(..., description="Search keyword"),
    ai_titles: str = Query(None, description="AI titles"),
    ai_summaries: str = Query(None, description="AI summaries"),
    title: str = Query(None, description="Title"),
    typenames: list[str] = Query(None, description="List of type names"),
    uploader: str = Query(None, description="Uploader"),
    pubdate_inf: int = Query(None, description="Publication date lower bound"),
    pubdate_sup: int = Query(None, description="Publication date upper bound"),
    tags_list: list[str] = Query(None, description="List of tags"),
    duration_inf: int = Query(None, description="Minimum duration"),
    duration_sup: int = Query(None, description="Maximum duration"),
    lan: str = Query(None, description="Language"),
    search_mode: int = Query(0, description="Search mode"),
    limit: int = Query(10, gt=0, description="Limit for pagination"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    try:
        sd = SearchData(
            df=df_raw,
            keyword_str=keyword,
            ai_titles=ai_titles,
            ai_summaries=ai_summaries,
            title=title,
            typenames=typenames,
            uploader=uploader,
            pubdate_inf=pubdate_inf,
            pubdate_sup=pubdate_sup,
            tags_list=tags_list,
            duration_inf=duration_inf,
            duration_sup=duration_sup,
            lan=lan,
            search_mode=search_mode,
            limit=limit,
            offset=offset
        )
        sd.get_results()
        dict_results = sd.get_json_results() # dict_results is a python list, not a json object
        del sd
        return dict_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
