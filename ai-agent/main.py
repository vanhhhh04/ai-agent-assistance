from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_community.utilities import SQLDatabase
from langchain_community.llms import Ollama
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain.prompts import PromptTemplate
import os

app = FastAPI(title="AI Data Assistant", version="1.0.0")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")  # ← reads from env

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
db = SQLDatabase.from_uri(
    DB_URL,
    include_tables=["orders","customers","products","order_items","payments","categories"]
)

llm = Ollama(
    model="llama3.2",
    base_url=OLLAMA_HOST,  # ← uses env variable not hardcoded localhost
    temperature=0
)

DANGEROUS_KEYWORDS = ["DROP","DELETE","UPDATE","INSERT","TRUNCATE","ALTER"]

SQL_PROMPT = PromptTemplate.from_template("""
You are a PostgreSQL expert. Given the database schema below, write ONLY a valid SQL SELECT query.
Do NOT explain anything. Do NOT write English sentences. Output ONLY the raw SQL query.

Database schema:
{schema}

Question: {question}

SQL Query (SELECT only, no markdown, no explanation):
""")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    question: str
    sql: str
    result: str
    error: str = None

@app.get("/")
def root():
    return {"message": "AI Data Assistant is running ✅"}

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model": "llama3.2",
        "ollama_host": OLLAMA_HOST,
        "tables": db.get_usable_table_names()
    }

@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    question = request.question
    for keyword in DANGEROUS_KEYWORDS:
        if keyword in question.upper():
            raise HTTPException(status_code=403, detail=f"Forbidden keyword: {keyword}")
    try:
        schema = db.get_table_info()
        prompt = SQL_PROMPT.format(schema=schema, question=question)
        sql = llm.invoke(prompt)
        sql = sql.strip()
        if "```sql" in sql:
            sql = sql.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql:
            sql = sql.split("```")[1].split("```")[0].strip()
        lines = sql.split("\n")
        sql_lines = [l for l in lines if l.strip().upper().startswith(
            ("SELECT","WITH","FROM","WHERE","GROUP","ORDER","HAVING","LIMIT","(")
        )]
        if sql_lines:
            sql = "\n".join(sql_lines)
        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";") + " LIMIT 100;"
        for keyword in DANGEROUS_KEYWORDS:
            if keyword in sql.upper():
                raise HTTPException(status_code=403, detail="Forbidden SQL operation")
        execute_tool = QuerySQLDataBaseTool(db=db)
        result = execute_tool.run(sql)
        return QueryResponse(question=question, sql=sql, result=result)
    except HTTPException:
        raise
    except Exception as e:
        return QueryResponse(question=question, sql=sql if 'sql' in locals() else "", result="", error=str(e))

@app.get("/schema")
def get_schema():
    return {"schema": db.get_table_info()}
